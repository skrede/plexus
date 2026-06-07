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
#include "plexus/io/mtu_budget.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/detail/udp_reliable_arq.h"
#include "plexus/io/detail/udp_handshake_frame.h"
#include "plexus/io/detail/udp_backpressure_queue.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace plexus::asio {

// The connectionless UDP byte_channel: plexus's first NON-STREAM channel. Unlike
// every stream channel (one kernel socket per connection) a udp_channel owns NO
// socket — it is a per-peer facade over the ONE router-owned udp_server, storing the
// destination endpoint plus the per-peer envelope/dedup state. send() wraps the
// frame in a udp_envelope (seq++, kind=best_effort) and calls server.send_to(dest);
// inbound is PUSHED in by the transport demux (deliver_inbound), not pulled by a
// self-run recv loop. The seven byte_channel verbs hold without reshaping the
// concept (the static_assert at file bottom is the load-bearing D2 proof).
//
// DIVERGENCE from the stream channels: no stream_inbound / frame_reassembler /
// slowloris timer (a datagram is a complete message — nothing to reassemble, no
// partial-frame stall to detect). The channel owns NO write queue (a datagram send is
// one-shot at the channel face); the shared udp_server owns the serial outbound queue
// that keeps each in-flight datagram's bytes alive across its async_send_to, so the
// channel hands send_to a scratch buffer it may reuse immediately on return.
// on_protocol_close is STORED and NEVER fired: the byte_channel concept
// licenses this for a non-stream channel ("no partial frame is expressible without a
// byte stream", byte_channel.h:43-46) — a malformed datagram is simply dropped.
//
// Oversize (D-03): a frame whose enveloped size exceeds max_payload is REJECTED at
// publish through on_error(message_too_large), never silently dropped — the channel
// stays open and the publisher learns the message will not send.
//
// The reliable_arq kind is a recv-side hook only this plan (the data ARQ is a later
// block); a kind=1 datagram is handed to m_on_reliable, which the transport leaves
// unset here so such datagrams are dropped until the ARQ engine is wired.
class udp_channel
{
public:
    // The per-channel payload budget the oversize-reject gates consult, relocated to the
    // shared io::mtu_budget value-object so the channel and any future datagram backend
    // read the SAME default (1400) instead of a scattered local literal. A caller MAY
    // override it at construction (the required-with-default ctor arg below).
    static constexpr std::size_t default_max_payload = io::mtu_budget{}.max_payload;
    // The bounded congestion=block backpressure queue depth (allocated at setup, never
    // grown on the hot path): a full send window AND a full queue surface a stall signal
    // rather than unbounded memory growth (T-15-13). A conservative multiple of the
    // default window — deep enough to ride a transient window-full burst, bounded so a
    // sustained overrun fails closed instead of OOMing.
    static constexpr std::size_t default_backpressure_depth = 1024;

    using arq_type = io::detail::udp_reliable_arq<::asio::io_context &, asio_timer>;

    // The reliable-ARQ config is a required-WITH-default ctor argument (the handshake-
    // ladder pattern): production binds the swept defaults; a deterministic test binds a
    // compressed config (a fast RTO / small cap) to exercise the SAME mechanics quickly.
    // The congestion mode is the per-channel QoS choice (block = the safe reliable
    // default; drop = the opt-out shed) threaded the same way; the backpressure depth
    // bounds the block queue.
    udp_channel(::asio::io_context &io, udp_server &server, ::asio::ip::udp::endpoint dest,
                std::size_t max_payload = default_max_payload,
                io::detail::udp_arq_config arq_cfg = {},
                io::congestion congestion = io::congestion::block,
                std::size_t backpressure_depth = default_backpressure_depth,
                io::detail::udp_channel_mode mode = io::detail::udp_channel_mode::best_effort)
        : m_io(io)
        , m_server(server)
        , m_dest(std::move(dest))
        , m_max_payload(max_payload)
        , m_arq_cfg(arq_cfg)
        , m_congestion(congestion)
        , m_backpressure(backpressure_depth)
        , m_mode(mode)
    {
    }

    udp_channel(const udp_channel &) = delete;
    udp_channel &operator=(const udp_channel &) = delete;
    udp_channel(udp_channel &&) = delete;
    udp_channel &operator=(udp_channel &&) = delete;

    // The dtor tears the channel down but never posts on_closed (a this-capturing
    // post could outlive the channel). close() posts on_closed. The ARQ's per-segment
    // retransmit timers are cancelled FIRST so a timer firing after the channel dies is
    // a cancelled no-op (the single-owner discipline — no shared_from_this).
    //
    // LIFETIME (the owner's teardown burden, heavier here than on a stream channel):
    // every inbound datagram posts a this-capturing delivery through post_on_data, so the
    // channel has MORE in-flight posted-`this` surface than a stream channel (which posts
    // only reassembled frames). The owner MUST drain/quiesce the executor before destroying
    // the channel — a posted inbound delivery still dereferences this->m_on_data when it
    // runs, and the dtor cannot cancel an already-posted handler (only the timers). This
    // matches the routing_engine LIFETIME note and the codebase-wide posted-`this` contract.
    ~udp_channel()
    {
        m_open = false;
        if(m_arq)
            m_arq->cancel();
    }

    // The single byte_channel send verb. A reliable_datagram-mode channel (the "udpr"
    // route) dispatches to the in-order ARQ; a best_effort-mode channel (the "udp" route)
    // is fire-and-forget. This is how the erased polymorphic_byte_channel — which exposes only send() —
    // engages the ARQ on the flipped "udpr" route without a separate reliable verb.
    void send(std::span<const std::byte> frame)
    {
        if(m_mode == io::detail::udp_channel_mode::reliable_datagram)
        {
            send_reliable(frame);
            return;
        }
        if(!m_open)
            return;
        if(frame.size() + wire::udp_envelope_overhead > m_max_payload)
            return reject_oversize();
        wire::wrap_udp_into(m_send_scratch, wire::udp_envelope_kind::best_effort, m_out_seq++, frame);
        m_server.send_to(m_send_scratch, m_dest);
    }

    void close()
    {
        if(!m_open)
            return;
        m_open = false;
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
    }

    // The scheme reflects the channel's mode so a route is provable end-to-end: a
    // best_effort channel reports "udp", a reliable_datagram channel reports "udpr". This
    // lets the mux's "udpr" -> UDP+ARQ flip be test-pinned (the erased channel reports
    // "udpr", proving it rode the datagram member in reliable mode, NOT the TCP stream).
    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        const char *scheme = m_mode == io::detail::udp_channel_mode::reliable_datagram ? "udpr" : "udp";
        return {scheme, m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
    }

    [[nodiscard]] io::detail::udp_channel_mode mode() const noexcept { return m_mode; }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // Submit a payload on the RELIABLE in-order path: the selective-repeat ARQ stamps a
    // seq, sends a kind=1 data segment, and retransmits under an adaptive RTO until the
    // peer acks. The congestion mode decides a FULL send window:
    //   * block (the safe reliable default): enqueue into the BOUNDED publish-side queue
    //     allocated at setup; the next ack (on_window_advance) drains it by re-submitting
    //     admissible frames, posted on the executor. publish() stays non-blocking — the
    //     reliable guarantee is preserved (no drop), the io_context is NEVER blocked. A
    //     queue at its cap surfaces would_block (the stall signal; never grows unbounded).
    //   * drop: shed the new frame at the publisher (the opt-out of the guarantee).
    // The ARQ is constructed lazily on first reliable use so a best_effort-only channel
    // pays nothing. Oversize is rejected at publish (the marker byte joins the overhead).
    using submit_result = arq_type::submit_result;

    submit_result send_reliable(std::span<const std::byte> payload)
    {
        if(!m_open)
            return submit_result::window_full;
        if(payload.size() + wire::udp_envelope_overhead + 1 > m_max_payload)
        {
            reject_oversize();
            return submit_result::window_full;
        }
        ensure_arq();
        const auto r = m_arq->submit(payload);
        if(r == submit_result::admitted)
            return r;
        return on_window_full(payload);          // block: enqueue/drain; drop: shed
    }

    // The reliable-ARQ recv hook (kind=1). The data ARQ is wired here: a kind=1 datagram
    // self-identifies (its inner control byte) as a data segment or an ack and is fanned
    // to the ARQ on ONE inbound demux path (the kind discriminator is also the
    // DTLS-bypass seam). An override may still observe raw reliable segments for tests.
    void on_reliable_segment(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> cb)
    {
        m_on_reliable = std::move(cb);
    }

    // Called BY the transport demux on each datagram for this peer — NOT a self-run
    // recv loop (the channel owns no socket). Strip the envelope, dedup best_effort,
    // post the inner frame. A malformed datagram is dropped (no on_protocol_close).
    void deliver_inbound(std::span<const std::byte> datagram)
    {
        auto dec = wire::unwrap_udp(datagram);
        if(!dec)
            return;                                  // malformed: drop, never on_protocol_close
        if(dec->kind == wire::udp_envelope_kind::best_effort)
        {
            if(m_dedup.admit(dec->seq) != wire::udp_dedup_window::outcome::fresh)
                return;                              // duplicate / too_old: drop
            post_on_data(dec->frame);
        }
        else if(m_mode == io::detail::udp_channel_mode::reliable_datagram)
        {
            deliver_reliable(dec->seq, dec->frame);
        }
        // else: a kind=1 datagram on a best_effort channel is not expected — DROP it.
        // The source endpoint is not trusted as identity, so routing it to the reliable
        // path would let a spoofed datagram spin up an unsolicited ARQ engine on a
        // fire-and-forget channel. Mode, not envelope kind alone, gates the engine.
    }

    [[nodiscard]] const ::asio::ip::udp::endpoint &dest() const noexcept { return m_dest; }
    [[nodiscard]] bool is_open() const noexcept { return m_open; }
    [[nodiscard]] io::congestion congestion_mode() const noexcept { return m_congestion; }
    // The count of frames shed under congestion=drop (the future drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }
    // The current backpressure-queue occupancy (congestion=block); 0 when the window drains.
    [[nodiscard]] std::size_t backpressured() const noexcept { return m_backpressure.size(); }

private:
    void reject_oversize()
    {
        if(m_on_error)
            m_on_error(io::io_error::message_too_large);
    }

    // Build the selective-repeat ARQ on first reliable use and wire its actions: a
    // (re)transmit wraps a kind=1 data segment, an ack wraps a kind=1 ack frame, an
    // in-order payload posts on_data, and exhaustion surfaces a connection-fatal error.
    void ensure_arq()
    {
        if(m_arq)
            return;
        m_arq = std::make_unique<arq_type>(m_io, m_arq_cfg);
        m_arq->on_transmit([this](std::uint16_t seq, std::span<const std::byte> payload) {
            wire::encode_udp_segment_into(m_arq_inner, payload);
            wire::wrap_udp_into(m_send_scratch, wire::udp_envelope_kind::reliable_arq, seq, m_arq_inner);
            m_server.send_to(m_send_scratch, m_dest);
        });
        m_arq->on_send_ack([this](const wire::udp_ack &ack) {
            wire::encode_udp_ack_into(m_arq_inner, ack);
            wire::wrap_udp_into(m_ack_scratch, wire::udp_envelope_kind::reliable_arq, 0, m_arq_inner);
            m_server.send_to(m_ack_scratch, m_dest);
        });
        m_arq->on_deliver([this](std::span<const std::byte> payload) { post_on_data(payload); });
        m_arq->on_exhausted([this] { if(m_on_error) m_on_error(io::io_error::timed_out); });
        // congestion=block: when an ack frees window slots, drain the backpressure queue
        // by re-submitting the admissible queued frames (the window-drain re-arm idiom,
        // mirroring unix_channel::do_write — never a blocking wait in the ack handler).
        m_arq->on_window_advance([this] { drain_backpressure(); });
    }

    // congestion=block enqueues a window-full reliable frame into the bounded queue (the
    // ack handler drains it); a queue at its cap surfaces would_block (the stall signal —
    // bounded, never unbounded growth, T-15-13). congestion=drop sheds the frame at the
    // publisher (the documented opt-out of the reliable guarantee). Either way publish()
    // stays non-blocking and the io_context is never blocked (T-15-12).
    submit_result on_window_full(std::span<const std::byte> payload)
    {
        if(m_congestion == io::congestion::drop)
        {
            ++m_dropped;                          // shed at the publisher (counted for the future observer)
            return submit_result::window_full;
        }
        if(!m_backpressure.admit(payload))
        {
            if(m_on_error)
                m_on_error(io::io_error::would_block);   // queue at cap: the stall edge
            return submit_result::window_full;
        }
        return submit_result::admitted;           // accepted into the queue; will send on the next ack
    }

    // Re-submit queued frames while the send window has room: each admit pops one. A
    // submit that returns window_full (a fresh fill between acks) stops the drain — the
    // remainder waits for the next on_window_advance. Bounded by the queue size; no
    // hot-path allocation beyond the at-setup queue storage.
    void drain_backpressure()
    {
        while(!m_backpressure.empty() && m_arq && m_arq->window_has_room())
        {
            if(m_arq->submit(m_backpressure.front()) != submit_result::admitted)
                break;
            m_backpressure.pop_front();
        }
    }

    // Fan a kind=1 inner frame to the ARQ on the ONE inbound demux path: a data segment
    // drives on_segment (in-order delivery + ack), an ack drives on_ack (window slide).
    // A test-installed raw observer (m_on_reliable) sees the segment too. A frame whose
    // marker is neither (a handshake byte, already split off upstream) is dropped.
    void deliver_reliable(std::uint16_t seq, std::span<const std::byte> inner)
    {
        auto kind = wire::peek_udp_arq_kind(inner);
        if(!kind)
            return;
        if(*kind == wire::udp_arq_kind::ack)
        {
            if(auto ack = wire::decode_udp_ack(inner); ack && m_arq)
                m_arq->on_ack(*ack);
            return;
        }
        auto payload = wire::decode_udp_segment(inner);
        if(!payload)
            return;
        if(m_on_reliable)
            m_on_reliable(seq, *payload);
        ensure_arq();
        m_arq->on_segment(seq, *payload);
    }

    // on_data is ALWAYS posted (the byte_channel contract). The owning vector keeps
    // the bytes alive across the post (the demux's recv buffer is reused immediately).
    void post_on_data(std::span<const std::byte> frame)
    {
        auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
        ::asio::post(m_io, [this, owned]
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{*owned});
        });
    }

    ::asio::io_context &m_io;
    udp_server &m_server;
    ::asio::ip::udp::endpoint m_dest;
    std::size_t m_max_payload;
    io::detail::udp_arq_config m_arq_cfg;
    io::congestion m_congestion;
    io::detail::udp_backpressure_queue m_backpressure;       // bounded congestion=block queue
    std::size_t m_dropped{0};                            // congestion=drop shed count
    io::detail::udp_channel_mode m_mode;                     // best_effort vs reliable_datagram
    std::uint16_t m_out_seq{0};
    wire::udp_dedup_window m_dedup;
    std::vector<std::byte> m_send_scratch;
    std::vector<std::byte> m_ack_scratch;
    std::vector<std::byte> m_arq_inner;
    std::unique_ptr<arq_type> m_arq;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_reliable;
    bool m_open{true};
};

}

static_assert(plexus::io::byte_channel<plexus::asio::udp_channel>,
    "udp_channel must satisfy byte_channel WITHOUT reshaping the concept — the NON-stream D2 proof");

#endif
