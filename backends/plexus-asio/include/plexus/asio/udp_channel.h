#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_CHANNEL_H

#include "plexus/asio/udp_server.h"
#include "plexus/asio/asio_timer.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"
#include "plexus/wire/udp_dedup_window.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/datagram/mtu_budget.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/fragmentation.h"
#include "plexus/datagram/detail/reassembler.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/datagram/detail/udp_reliable_arq.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"
#include "plexus/datagram/detail/udp_backpressure_queue.h"
#include "plexus/asio/detail/udp_channel_io.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::asio {

// The connectionless UDP byte_channel: a udp_channel owns NO socket — it is a per-peer facade over
// the ONE router-owned udp_server, storing the destination endpoint plus the per-peer
// envelope/dedup state. Inbound is PUSHED in by the transport demux (deliver_inbound). A frame past
// max_payload is FRAGMENTED across numbered datagrams; a frame past the max-message size is
// REJECTED at publish. on_protocol_close is STORED and NEVER fired (a malformed datagram is dropped
// — the byte_channel concept licenses this for a non-stream channel).
class udp_channel
{
public:
    static constexpr std::size_t default_max_payload = datagram::mtu_budget{}.max_payload;
    // The not-yet-windowed fragments of a paced reliable message park here while the ARQ send
    // window is full. This bounds ADDITIONAL backlog ONLY: the per-message ceiling is the sole
    // size authority, so the live cap is floored at that ceiling (see m_backpressure construction)
    // and a within-ceiling message's whole fragment backlog is always admissible. The default
    // sizes the EXTRA backlog a producer outrunning the drain may pile up past one in-flight
    // message.
    static constexpr std::size_t default_backpressure_bytes = 1u * io::fragmentation_limits::max_message_size;

    using arq_type         = datagram::detail::udp_reliable_arq<::asio::io_context &, asio_timer>;
    using reassembler_type = datagram::detail::reassembler<::asio::io_context &, asio_timer>;

    // max_message_bytes is the per-MESSAGE size ceiling (send-side oversize-reject AND the receive
    // reassembler's per-message ceiling), distinct from max_payload (the per-FRAGMENT MTU budget).
    udp_channel(::asio::io_context &io, udp_server &server, ::asio::ip::udp::endpoint dest, std::size_t max_payload = default_max_payload, datagram::detail::udp_arq_config arq_cfg = {},
                io::congestion congestion = io::congestion::block, std::size_t backpressure_bytes = default_backpressure_bytes,
                datagram::detail::udp_channel_mode mode = datagram::detail::udp_channel_mode::best_effort, std::uint16_t initial_seq = 0,
                std::size_t max_message_bytes = io::global_default_max_message_bytes, std::size_t reassembly_budget = io::reassembly_memory_budget,
                std::chrono::milliseconds reassembly_timeout = reassembler_type::config{}.per_message_timeout)
            : m_io(io)
            , m_server(server)
            , m_dest(std::move(dest))
            , m_max_payload(max_payload)
            , m_max_message_bytes(max_message_bytes)
            , m_reassembly_budget(reassembly_budget)
            , m_reassembly_timeout(reassembly_timeout)
            , m_arq_cfg(arq_cfg)
            , m_congestion(congestion)
            // Floor the cap at the per-message ceiling: a single within-ceiling message's full
            // fragment backlog must always be admissible regardless of the back-pressure knob.
            , m_backpressure(std::max(backpressure_bytes, max_message_bytes))
            , m_dropped(0)
            , m_mode(mode)
            , m_initial_seq(initial_seq)
            , m_scheduler_key(io::detail::next_scheduler_key())
            , m_out_seq(0)
            , m_out_msg_id(0)
            , m_open(true)
    {
    }

    udp_channel(const udp_channel &)            = delete;
    udp_channel &operator=(const udp_channel &) = delete;
    udp_channel(udp_channel &&)                 = delete;
    udp_channel &operator=(udp_channel &&)      = delete;

    // Never posts on_closed (a this-capturing post could outlive the channel); close() does. The
    // ARQ retransmit timers are cancelled FIRST so a timer firing after the channel dies is a
    // cancelled no-op.
    //
    // LIFETIME: every inbound datagram posts a this-capturing delivery through post_on_data, so the
    // owner MUST drain/quiesce the executor before destroying the channel — the dtor cannot cancel
    // an already-posted handler (only the timers).
    ~udp_channel()
    {
        m_open = false;
        if(m_reassembler)
            m_reassembler->cancel();
        if(m_arq)
            m_arq->cancel();
        // Erase the transport demux ref BEFORE this object dies.
        if(m_on_teardown_cb)
            m_on_teardown_cb();
    }

    // A reliable_datagram-mode channel dispatches to the in-order ARQ; a best_effort-mode channel
    // is fire-and-forget. This is how the erased polymorphic_byte_channel (which exposes only
    // send()) engages the ARQ on the "udpr" route without a separate reliable verb.
    void send(std::span<const std::byte> frame)
    {
        if(m_mode == datagram::detail::udp_channel_mode::reliable_datagram)
        {
            send_reliable(frame);
            return;
        }
        if(!m_open)
            return;
        if(frame.size() + wire::udp_envelope_overhead > m_max_payload)
            return detail::send_best_effort_large(*this, frame);
        // A whole single datagram: idle fast-path eligible.
        wire::wrap_udp_into(m_send_scratch, wire::udp_envelope_kind::best_effort, m_out_seq++, frame);
        m_server.send_standalone_to(m_send_scratch, m_dest);
    }

    void close()
    {
        if(!m_open)
            return;
        m_open = false;
        // Posted, never synchronous: a this-capturing on_closed could otherwise run inline.
        ::asio::post(m_io,
                     [this]
                     {
                         if(m_on_closed_cb)
                             m_on_closed_cb();
                     });
    }

    // The scheme reflects the channel's mode: a best_effort channel reports "udp", a
    // reliable_datagram channel reports "udpr".
    io::endpoint remote_endpoint() const
    {
        const char *scheme = m_mode == datagram::detail::udp_channel_mode::reliable_datagram ? "udpr" : "udp";
        return {scheme, m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
    }

    datagram::detail::udp_channel_mode mode() const noexcept
    {
        return m_mode;
    }

    // The negotiated per-session ISN (RFC 6528) the receiver expects as its first in-order seq;
    // 0 on the legacy default. A seq strictly below this is a provable duplicate.
    std::uint16_t initial_seq() const noexcept
    {
        return m_initial_seq;
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    // An ARQ shed at the publisher emits here; the lazily-built reassembler's drop sink is
    // forwarded onto this one. The sink POSTS, so no shed site fires the observer synchronously.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    // The transport's private teardown seam: the transport demuxes inbound by endpoint to a
    // NON-owning raw ref, and erases that ref here when the owner destroys the channel, so a later
    // datagram is a clean MISS rather than a freed-pointer deref.
    void on_teardown(plexus::detail::move_only_function<void()> cb)
    {
        m_on_teardown_cb = std::move(cb);
    }

    // The selective-repeat ARQ stamps a seq, sends a data segment, and retransmits under an
    // adaptive RTO until the peer acks. On a full window: block enqueues into the bounded
    // publish-side queue (the next ack drains it, posted on the executor — non-blocking, no drop),
    // drop sheds the new frame. The ARQ is constructed lazily on first reliable use so a
    // best_effort-only channel pays nothing.
    using submit_result = arq_type::submit_result;

    submit_result send_reliable(std::span<const std::byte> payload)
    {
        if(!m_open)
            return submit_result::window_full;
        if(payload.size() + wire::udp_envelope_overhead + 1 > m_max_payload)
            return detail::send_reliable_large(*this, payload);
        detail::ensure_arq(*this);
        const auto r = m_arq->submit(payload);
        if(r == submit_result::admitted)
            return r;
        return detail::on_window_full(*this, payload, /*fragmented=*/false);
    }

    void on_reliable_segment(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> cb)
    {
        m_on_reliable_cb = std::move(cb);
    }

    // Called BY the transport demux on each datagram for this peer (the channel owns no socket): it
    // strips the envelope, dedups best_effort, and posts the inner frame. A malformed datagram is
    // dropped (no on_protocol_close).
    void deliver_inbound(std::span<const std::byte> datagram)
    {
        detail::deliver_inbound(*this, datagram);
    }

    // Unique per object, so a reconnect at a reused heap address cannot bleed a stale band entry
    // into the egress scheduler's band map.
    std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    const ::asio::ip::udp::endpoint &dest() const noexcept
    {
        return m_dest;
    }
    bool is_open() const noexcept
    {
        return m_open;
    }
    io::congestion congestion_mode() const noexcept
    {
        return m_congestion;
    }
    std::size_t dropped_count() const noexcept
    {
        return m_dropped;
    }
    // Byte-valued (not the frame count) so it shares the stream channel's occupancy contract and
    // the egress scheduler's byte-denominated low-water gate compares like with like.
    std::size_t backpressured() const noexcept
    {
        return m_backpressure.queued_bytes();
    }
    std::size_t write_queue_capacity() const noexcept
    {
        return m_backpressure.capacity();
    }

    // The per-MESSAGE size ceiling (the send-side oversize-reject bound and the receive
    // reassembler's per-message cap). The splice gates an outbound forwarded envelope against this,
    // so a frame too large for this leg drops-with-count instead of being rejected at publish.
    std::size_t max_frame_bytes() const noexcept
    {
        return m_max_message_bytes;
    }

private:
    template<typename Ch>
    friend void detail::reject_oversize(Ch &);
    template<typename Ch>
    friend bool detail::exceeds_max_message(const Ch &, std::size_t) noexcept;
    template<typename Ch>
    friend void detail::send_best_effort_large(Ch &, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::post_on_data(Ch &, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::post_on_data_owned(Ch &, wire::shared_bytes);
    template<typename Ch>
    friend void detail::ensure_reassembler(Ch &);
    template<typename Ch>
    friend void detail::feed_fragment(Ch &, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::deliver_reliable_inorder(Ch &, bool, std::span<const std::byte>);
    template<typename Ch>
    friend typename Ch::submit_result detail::on_window_full(Ch &, std::span<const std::byte>, bool);
    template<typename Ch>
    friend typename Ch::submit_result detail::submit_reliable_fragment(Ch &, std::uint16_t, std::uint32_t, std::uint32_t, std::span<const std::byte>);
    template<typename Ch>
    friend typename Ch::submit_result detail::send_reliable_large(Ch &, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::drain_backpressure(Ch &);
    template<typename Ch>
    friend void detail::ensure_arq(Ch &);
    template<typename Ch>
    friend void detail::deliver_reliable(Ch &, std::uint16_t, bool, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::deliver_inbound(Ch &, std::span<const std::byte>);

    ::asio::io_context &m_io;
    udp_server &m_server;
    ::asio::ip::udp::endpoint m_dest;
    std::size_t m_max_payload;                      // per-FRAGMENT MTU budget (NOT the message ceiling)
    std::size_t m_max_message_bytes;                // per-MESSAGE size ceiling (send + receive)
    std::size_t m_reassembly_budget;                // aggregate reassembly-memory cap (always-on)
    std::chrono::milliseconds m_reassembly_timeout; // per-message reassembly reclaim window
    datagram::detail::udp_arq_config m_arq_cfg;
    io::congestion m_congestion;
    datagram::detail::udp_backpressure_queue m_backpressure;
    std::size_t m_dropped;
    datagram::detail::udp_channel_mode m_mode;
    std::uint16_t m_initial_seq; // negotiated per-session ISN (RFC 6528); 0 = legacy
    std::uint64_t m_scheduler_key;
    std::uint16_t m_out_seq;
    std::uint16_t m_out_msg_id;
    wire::udp_dedup_window m_dedup;
    std::vector<std::byte> m_send_scratch;
    std::vector<std::byte> m_ack_scratch;
    std::vector<std::byte> m_arq_inner;
    std::vector<std::byte> m_frag_scratch;
    std::unique_ptr<arq_type> m_arq;
    std::unique_ptr<reassembler_type> m_reassembler;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void()> m_on_closed_cb;
    plexus::detail::move_only_function<void()> m_on_teardown_cb;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_reliable_cb;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;
    bool m_open;
};

}

static_assert(plexus::io::byte_channel<plexus::asio::udp_channel>, "udp_channel must satisfy byte_channel WITHOUT reshaping the concept (the non-stream channel)");

#endif
