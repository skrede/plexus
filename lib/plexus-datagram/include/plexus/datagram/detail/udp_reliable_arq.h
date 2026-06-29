#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_RELIABLE_ARQ_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_RELIABLE_ARQ_H

#include "plexus/policy.h"

#include "plexus/datagram/detail/udp_rto_estimator.h"
#include "plexus/datagram/detail/udp_reorder_buffer.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/udp_ack.h"

#include <span>
#include <memory>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::datagram::detail {

// A selective-repeat sliding-window sender + a head-of-line reorder receiver + an adaptive
// RFC 6298 RTO (Karn's algorithm): every reliable datagram is delivered in publish order,
// retransmitting only the lost segment (selective-repeat, not go-back-N).
struct udp_arq_config
{
    // The selective-ack bitmap (wire::udp_ack::bitmap_bits) names only that many holes above
    // the cumulative edge; holes beyond a window LARGER than bitmap_bits fall back to
    // per-segment RTO-driven retransmit (idempotent at the receiver).
    std::size_t window{512};                    // in-flight segment bound (<= 2^15)
    std::chrono::milliseconds initial_rto{200}; // before any RTT sample
    std::chrono::milliseconds min_rto{50};
    std::chrono::milliseconds max_rto{2000};
    std::uint8_t max_retransmit{6}; // cap -> connection-fatal on exhaustion
};

static_assert(udp_arq_config{}.window <= wire::udp_ack::bitmap_bits * 2,
              "default ARQ window outruns the selective-ack bitmap by more than the "
              "RTO-fallback design budget — widen wire::udp_ack::bitmap_bits with it");

template<typename Executor, typename Timer>
    requires plexus::timer<Timer> && std::constructible_from<Timer, Executor>
class udp_reliable_arq
{
public:
    using timer_type = Timer;
    using clock      = std::chrono::steady_clock;
    using ms         = std::chrono::milliseconds;

    enum class submit_result : std::uint8_t
    {
        admitted,
        window_full
    };

    // CONTRACT: both ends start at the same initial_seq, so the cumulative-ack edge is
    // meaningful from the first segment.
    explicit udp_reliable_arq(Executor executor, udp_arq_config cfg = {}, std::uint16_t initial_seq = 0)
            : m_executor(executor)
            , m_cfg(cfg)
            , m_window(cfg.window == 0 ? std::size_t{512} : std::min(cfg.window, std::size_t{32768}))
            , m_slots(m_window)
            , m_recv(m_window, initial_seq)
            , m_next(initial_seq)
            , m_base(initial_seq)
            , m_rto(cfg.initial_rto, cfg.min_rto, cfg.max_rto)
    {
        for(auto &s : m_slots)
            s.timer = std::make_unique<timer_type>(m_executor);
        m_recv.on_deliver(
                [this](std::uint16_t seq, bool fragmented, std::span<const std::byte> bytes)
                {
                    if(m_on_deliver_seq)
                        m_on_deliver_seq(seq, fragmented, bytes);
                    if(m_on_deliver)
                        m_on_deliver(bytes);
                });
    }

    udp_reliable_arq(const udp_reliable_arq &)            = delete;
    udp_reliable_arq &operator=(const udp_reliable_arq &) = delete;

    void on_transmit(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>, bool)> cb)
    {
        m_on_transmit = std::move(cb);
    }

    void on_send_ack(plexus::detail::move_only_function<void(const wire::udp_ack &)> cb)
    {
        m_on_send_ack = std::move(cb);
    }

    void on_deliver(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_deliver = std::move(cb);
    }

    void on_deliver_seq(plexus::detail::move_only_function<void(std::uint16_t, bool, std::span<const std::byte>)> cb)
    {
        m_on_deliver_seq = std::move(cb);
    }

    void on_exhausted(plexus::detail::move_only_function<void()> cb)
    {
        m_on_exhausted = std::move(cb);
    }

    void on_window_advance(plexus::detail::move_only_function<void()> cb)
    {
        m_on_window_advance = std::move(cb);
    }

    bool window_has_room() const noexcept
    {
        return in_flight() < m_window;
    }

    submit_result submit(std::span<const std::byte> payload, bool fragmented = false)
    {
        if(in_flight() >= m_window)
            return submit_result::window_full;
        const std::uint16_t seq = m_next++;
        auto &slot              = m_slots[seq % m_window];
        slot.bytes.assign(payload.begin(), payload.end());
        slot.outstanding   = true;
        slot.retransmitted = false;
        slot.retransmits   = 0;
        slot.fragmented    = fragmented;
        slot.sent_at       = clock::now();
        transmit(seq, slot);
        arm(seq);
        return submit_result::admitted;
    }

    void on_segment(std::uint16_t seq, bool fragmented, std::span<const std::byte> payload)
    {
        m_recv.feed(seq, fragmented, payload);
        send_ack();
    }

    void on_ack(const wire::udp_ack &ack)
    {
        slide_to(static_cast<std::uint16_t>(ack.cumulative + 1));
        // The bitmap names holes above the cumulative edge: offset i -> seq (cumulative + 2 + i).
        for(std::size_t i = 0; i < wire::udp_ack::bitmap_bits; ++i)
        {
            if(!ack.hole_received(i))
                continue;
            const auto seq = static_cast<std::uint16_t>(ack.cumulative + 2 + i);
            ack_hole(seq);
        }
    }

    void cancel()
    {
        m_dead = true;
        for(auto &s : m_slots)
            s.timer->cancel();
    }

    std::size_t in_flight() const noexcept
    {
        return static_cast<std::uint16_t>(m_next - m_base);
    }
    ms rto() const noexcept
    {
        return m_rto.rto();
    }

private:
    struct slot
    {
        std::vector<std::byte> bytes;
        std::unique_ptr<timer_type> timer;
        clock::time_point sent_at;
        std::uint8_t retransmits{0};
        bool outstanding{false};
        bool retransmitted{false};
        bool fragmented{false};
    };

    void transmit(std::uint16_t seq, slot &s)
    {
        if(m_on_transmit)
            m_on_transmit(seq, std::span<const std::byte>{s.bytes}, s.fragmented);
    }

    void arm(std::uint16_t seq)
    {
        auto &s = m_slots[seq % m_window];
        s.timer->expires_after(m_rto.backed_off(s.retransmits)); // Karn backoff per retransmit
        s.timer->async_wait(
                [this, seq](std::error_code ec)
                {
                    if(ec || m_dead)
                        return; // cancelled by ack/teardown — never fire dead
                    on_rto(seq);
                });
    }

    void on_rto(std::uint16_t seq)
    {
        auto &s = m_slots[seq % m_window];
        if(!s.outstanding)
            return; // already acked between fire and dispatch
        if(s.retransmits >= m_cfg.max_retransmit)
            return exhausted();
        ++s.retransmits;
        s.retransmitted = true; // Karn: this segment yields no RTT sample
        transmit(seq, s);
        arm(seq);
    }

    void slide_to(std::uint16_t new_base)
    {
        const std::uint16_t before = m_base;
        // Never slide the base past next-to-send: an ack ahead of the highest-sent seq would
        // otherwise underflow in_flight() and wedge the window at window_full.
        while(static_cast<std::uint16_t>(new_base - m_base) != 0 && static_cast<std::uint16_t>(new_base - m_base) < udp_reorder_buffer::half_space && m_base != m_next)
        {
            free_segment(m_base, /*sample_rtt=*/true);
            ++m_base;
        }
        if(m_base != before && m_on_window_advance)
            m_on_window_advance();
    }

    // A selectively-acked hole above the base: cancel its retransmit without advancing the
    // base (the gap below it is still unacked).
    void ack_hole(std::uint16_t seq)
    {
        const auto rel = static_cast<std::uint16_t>(seq - m_base);
        if(rel >= udp_reorder_buffer::half_space || rel >= m_window)
            return; // below base (already freed) or out of window
        free_segment(seq, /*sample_rtt=*/true);
    }

    void free_segment(std::uint16_t seq, bool sample_rtt)
    {
        auto &s = m_slots[seq % m_window];
        if(!s.outstanding)
            return;
        s.timer->cancel();
        if(sample_rtt && !s.retransmitted) // Karn: only an unambiguous (un-retransmitted) sample
            m_rto.sample(std::chrono::duration_cast<ms>(clock::now() - s.sent_at));
        s.outstanding = false;
    }

    void send_ack()
    {
        if(!m_on_send_ack)
            return;
        wire::udp_ack ack;
        ack.cumulative = m_recv.cumulative();
        // Bitmap offset i names seq (cumulative + 2 + i), i.e. reorder offset (i + 1); the
        // sender decode (on_ack) uses the identical mapping.
        for(std::size_t i = 0; i < wire::udp_ack::bitmap_bits; ++i)
            if(m_recv.buffered_at(i + 1))
                ack.mark_hole(i);
        m_on_send_ack(ack);
    }

    void exhausted()
    {
        if(m_dead)
            return;
        m_dead = true;
        if(m_on_exhausted)
            m_on_exhausted();
    }

    Executor m_executor;
    udp_arq_config m_cfg;
    std::size_t m_window;
    std::vector<slot> m_slots;
    udp_reorder_buffer m_recv;
    std::uint16_t m_next{0};
    std::uint16_t m_base{0};
    udp_rto_estimator m_rto;
    bool m_dead{false};
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>, bool)> m_on_transmit;
    plexus::detail::move_only_function<void(const wire::udp_ack &)> m_on_send_ack;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_deliver;
    plexus::detail::move_only_function<void(std::uint16_t, bool, std::span<const std::byte>)> m_on_deliver_seq;
    plexus::detail::move_only_function<void()> m_on_exhausted;
    plexus::detail::move_only_function<void()> m_on_window_advance;
};

}

#endif
