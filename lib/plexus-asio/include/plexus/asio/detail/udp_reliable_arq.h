#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_RELIABLE_ARQ_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_RELIABLE_ARQ_H

#include "plexus/asio/detail/udp_reorder_buffer.h"
#include "plexus/asio/detail/udp_rto_estimator.h"

#include "plexus/wire/udp_ack.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <memory>

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::asio::detail {

// The reliable-data ARQ engine: a selective-repeat sliding-window sender + a head-of-
// line reorder receiver + an adaptive RFC-6298 RTO (Karn's algorithm), designed fresh
// from standard selective-repeat literature (no prior-art port — the proven core
// forbade data retransmit). Reverses that invariant: every reliable datagram is
// delivered AND delivered in publish order (D-04), retransmitting ONLY the lost
// segment (selective-repeat, not go-back-N).
//
// The named constants below are PLACEHOLDER defaults to be SUBSTANTIATED BY SWEEP (a
// later loss-injection sweep sets the production numbers from recorded evidence, not by
// feel): the initial/min/max RTO, the window depth W, and the per-segment retransmit
// cap. They are gathered here so the sweep has one place to vary.
//
// LIFETIME: single-owner, bare `this`, NO shared_from_this / strand / static singleton
// (the same discipline as the handshake ARQ). The owning channel cancels every
// per-segment timer (cancel(), driven from the channel dtor) before the engine dies, so
// a timer firing after teardown is a cancelled no-op (ec -> the if(ec) guard returns).
// The send window + per-segment storage are allocated once at setup — no steady-state
// hot-path allocation.
//
// Templated on the executor + timer types directly (NOT the full Policy) so the
// udp_channel can own it without an include cycle (udp_policy.h includes udp_channel.h):
// the engine needs only an executor to build timers from and a plexus::timer surface,
// not a byte_channel. The timer must be constructible from the executor (the backend
// convention every plexus timer follows).
// The adaptive-RTO + window + retransmit-cap numbers gathered in ONE place. These are
// PLACEHOLDER defaults to be SUBSTANTIATED BY SWEEP (a later loss-injection sweep sets
// the production numbers from recorded evidence, not by feel). It is a required-WITH-
// default ctor argument (the handshake-ladder pattern): production binds the defaults; a
// deterministic test binds a compressed config to exercise the SAME mechanics quickly —
// NOT a mutable test-only setter.
struct udp_arq_config
{
    std::size_t window{512};                                 // in-flight segment bound (<= 2^15)
    std::chrono::milliseconds initial_rto{200};              // before any RTT sample
    std::chrono::milliseconds min_rto{50};
    std::chrono::milliseconds max_rto{2000};
    std::uint8_t max_retransmit{6};                          // cap -> connection-fatal on exhaustion
};

template <typename Executor, typename Timer>
    requires plexus::timer<Timer> && std::constructible_from<Timer, Executor>
class udp_reliable_arq
{
public:
    using timer_type = Timer;
    using clock = std::chrono::steady_clock;
    using ms = std::chrono::milliseconds;

    enum class submit_result : std::uint8_t { admitted, window_full };

    explicit udp_reliable_arq(Executor executor, udp_arq_config cfg = {})
        : m_executor(executor)
        , m_cfg(cfg)
        , m_window(cfg.window == 0 ? std::size_t{512} : std::min(cfg.window, std::size_t{32768}))
        , m_slots(m_window)
        , m_recv(m_window)
        , m_rto(cfg.initial_rto, cfg.min_rto, cfg.max_rto)
    {
        for(auto &s : m_slots)
            s.timer = std::make_unique<timer_type>(m_executor);
        m_recv.on_deliver([this](std::uint16_t, std::span<const std::byte> bytes) {
            if(m_on_deliver)
                m_on_deliver(bytes);
        });
    }

    udp_reliable_arq(const udp_reliable_arq &) = delete;
    udp_reliable_arq &operator=(const udp_reliable_arq &) = delete;

    // (re)transmit segment `seq` carrying `payload` (the channel wraps the envelope).
    void on_transmit(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> cb) { m_on_transmit = std::move(cb); }
    // send the receiver's cumulative+selective ack back to the sender.
    void on_send_ack(plexus::detail::move_only_function<void(const wire::udp_ack &)> cb) { m_on_send_ack = std::move(cb); }
    // an in-order reliable payload is ready for the application (the channel posts it).
    void on_deliver(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_deliver = std::move(cb); }
    // the retransmit cap was hit -> the reliable guarantee cannot be met (fatal).
    void on_exhausted(plexus::detail::move_only_function<void()> cb) { m_on_exhausted = std::move(cb); }

    // Admit a payload into the bounded send window. A full window returns window_full
    // (the congestion path is a later block; this never blocks the io_context). On
    // admit: stamp the next seq, store for retransmit, send, arm the per-segment RTO.
    submit_result submit(std::span<const std::byte> payload)
    {
        if(in_flight() >= m_window)
            return submit_result::window_full;
        const std::uint16_t seq = m_next++;
        auto &slot = m_slots[seq % m_window];
        slot.bytes.assign(payload.begin(), payload.end());
        slot.outstanding = true;
        slot.retransmitted = false;
        slot.retransmits = 0;
        slot.sent_at = clock::now();
        transmit(seq, slot);
        arm(seq);
        return submit_result::admitted;
    }

    // A data segment arrived: drive the in-order reorder buffer, then ack what we have.
    void on_segment(std::uint16_t seq, std::span<const std::byte> payload)
    {
        m_recv.feed(seq, payload);
        send_ack();
    }

    // An ack arrived: slide the window past the cumulative edge (freeing + RTT-sampling
    // never-retransmitted segments), then cancel any selectively-acked holes.
    void on_ack(const wire::udp_ack &ack)
    {
        slide_to(static_cast<std::uint16_t>(ack.cumulative + 1));
        // The bitmap names holes above the cumulative edge: offset i -> seq
        // (cumulative + 2 + i). A set bit is a received hole -> cancel its retransmit.
        for(std::size_t i = 0; i < wire::udp_ack::bitmap_bits; ++i)
        {
            if(!ack.hole_received(i))
                continue;
            const auto seq = static_cast<std::uint16_t>(ack.cumulative + 2 + i);
            ack_hole(seq);
        }
    }

    // The receiver's initial expected seq must match the sender's first seq so the
    // cumulative-ack edge is meaningful from the first segment.
    void set_initial(std::uint16_t initial) noexcept
    {
        m_next = initial;
        m_base = initial;
        m_recv.set_initial(initial);
    }

    // Owner teardown: cancel every per-segment timer (single-owner discipline).
    void cancel()
    {
        m_dead = true;
        for(auto &s : m_slots)
            s.timer->cancel();
    }

    [[nodiscard]] std::size_t in_flight() const noexcept
    {
        return static_cast<std::uint16_t>(m_next - m_base);
    }
    [[nodiscard]] ms rto() const noexcept { return m_rto.rto(); }

private:
    struct slot
    {
        std::vector<std::byte> bytes;
        std::unique_ptr<timer_type> timer;
        clock::time_point sent_at;
        std::uint8_t retransmits{0};
        bool outstanding{false};
        bool retransmitted{false};
    };

    void transmit(std::uint16_t seq, slot &s)
    {
        if(m_on_transmit)
            m_on_transmit(seq, std::span<const std::byte>{s.bytes});
    }

    void arm(std::uint16_t seq)
    {
        auto &s = m_slots[seq % m_window];
        s.timer->expires_after(m_rto.backed_off(s.retransmits));   // Karn backoff per retransmit
        s.timer->async_wait([this, seq](std::error_code ec) {
            if(ec || m_dead)
                return;                          // cancelled by ack/teardown — never fire dead
            on_rto(seq);
        });
    }

    void on_rto(std::uint16_t seq)
    {
        auto &s = m_slots[seq % m_window];
        if(!s.outstanding)
            return;                              // already acked between fire and dispatch
        if(s.retransmits >= m_cfg.max_retransmit)
            return exhausted();                  // the reliable guarantee cannot be met
        ++s.retransmits;
        s.retransmitted = true;                  // Karn: this segment yields no RTT sample
        transmit(seq, s);
        arm(seq);
    }

    // Cumulative ack: free + cancel every outstanding segment in [m_base, new_base).
    void slide_to(std::uint16_t new_base)
    {
        while(static_cast<std::uint16_t>(new_base - m_base) != 0
              && static_cast<std::uint16_t>(new_base - m_base) < udp_reorder_buffer::half_space)
        {
            free_segment(m_base, /*sample_rtt=*/true);
            ++m_base;
        }
    }

    // A selectively-acked hole above the base: cancel its retransmit so it is not
    // resent, but do NOT advance the base (the gap below it is still unacked).
    void ack_hole(std::uint16_t seq)
    {
        const auto rel = static_cast<std::uint16_t>(seq - m_base);
        if(rel >= udp_reorder_buffer::half_space || rel >= m_window)
            return;                              // below base (already freed) or out of window
        free_segment(seq, /*sample_rtt=*/true);
    }

    void free_segment(std::uint16_t seq, bool sample_rtt)
    {
        auto &s = m_slots[seq % m_window];
        if(!s.outstanding)
            return;
        s.timer->cancel();
        if(sample_rtt && !s.retransmitted)       // Karn: only an unambiguous (un-retransmitted) sample
            m_rto.sample(std::chrono::duration_cast<ms>(clock::now() - s.sent_at));
        s.outstanding = false;
    }

    void send_ack()
    {
        if(!m_on_send_ack)
            return;
        wire::udp_ack ack;
        ack.cumulative = m_recv.cumulative();
        // Bitmap offset i names seq (cumulative + 2 + i) == (expected + 1 + i). Since
        // expected == cumulative + 1 is the gap itself (reorder offset 0), that seq is
        // reorder offset (i + 1). Sender decode (on_ack) uses the identical mapping.
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
    std::vector<slot> m_slots;                   // a ring of W slots, allocated at setup
    udp_reorder_buffer m_recv;
    std::uint16_t m_next{0};                      // next seq to assign (sender)
    std::uint16_t m_base{0};                      // oldest unacked seq (sender)
    udp_rto_estimator m_rto;                      // RFC-6298 SRTT/RTTVAR -> RTO (Karn at the caller)
    bool m_dead{false};
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_transmit;
    plexus::detail::move_only_function<void(const wire::udp_ack &)> m_on_send_ack;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_deliver;
    plexus::detail::move_only_function<void()> m_on_exhausted;
};

}

#endif
