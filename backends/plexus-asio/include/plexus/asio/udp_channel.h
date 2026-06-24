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

// over-limit: one cohesive UDP byte_channel; the public verbs + the per-peer envelope/dedup/ARQ/
// reassembly/backpressure state are one whole (the byte_channel concept proof at file bottom
// binds the surface together), so splitting the public face scatters that shared state — the
// send/recv path + ARQ glue is already extracted to detail/udp_channel_io.h.
//
// The connectionless UDP byte_channel: plexus's first NON-STREAM channel. A udp_channel owns NO
// socket — it is a per-peer facade over the ONE router-owned udp_server, storing the destination
// endpoint plus the per-peer envelope/dedup state. send() wraps the frame in a udp_envelope and
// calls server.send_to(dest); inbound is PUSHED in by the transport demux (deliver_inbound). A
// frame past max_payload is FRAGMENTED across numbered datagrams; only a frame past the bounded
// max-message size is REJECTED at publish. on_protocol_close is STORED and NEVER fired (the
// byte_channel concept licenses this for a non-stream channel — a malformed datagram is dropped).
class udp_channel
{
public:
    // The per-channel payload budget the oversize-reject gates consult, relocated to the
    // shared datagram::mtu_budget value-object so the channel and any future datagram backend
    // read the SAME default instead of a scattered local literal. A caller MAY override it
    // at construction (the required-with-default ctor arg below).
    static constexpr std::size_t default_max_payload = datagram::mtu_budget{}.max_payload;
    // The bounded congestion=block backpressure BYTE budget (allocated at setup, never
    // grown on the hot path): the not-yet-windowed fragments of a paced reliable message
    // park here while the ARQ send window is full, drained by the next ack. This cap bounds
    // ADDITIONAL backlog ONLY — it is a local back-pressure resource knob, NOT a message-size
    // authority: the negotiated per-message ceiling (effective_max, enforced at publish) is
    // the SOLE bound on a single message's size, so the live cap is floored at that ceiling
    // (see m_backpressure construction) and a within-ceiling message's whole fragment backlog
    // is always admissible regardless of this default. The default sizes the EXTRA backlog a
    // producer outrunning the drain may pile up beyond one in-flight message; a sustained
    // overrun still fails closed at the floored cap. A load-bearing knob — to be substantiated
    // at the fan-out benchmark, not fixed by feel.
    static constexpr std::size_t default_backpressure_bytes = 1u * io::fragmentation_limits::max_message_size;

    using arq_type         = datagram::detail::udp_reliable_arq<::asio::io_context &, asio_timer>;
    using reassembler_type = datagram::detail::reassembler<::asio::io_context &, asio_timer>;

    // The reliable-ARQ config is a required-WITH-default ctor argument (the handshake-
    // ladder pattern): production binds the swept defaults; a deterministic test binds a
    // compressed config (a fast RTO / small cap) to exercise the SAME mechanics quickly.
    // The congestion mode is the per-channel QoS choice (block = the safe reliable
    // default; drop = the opt-out shed) threaded the same way; the backpressure byte
    // budget bounds the block queue.
    // max_message_bytes is the per-MESSAGE size ceiling (send-side oversize-reject AND
    // the receive reassembler's per-message ceiling) — the channel's effective-max,
    // distinct from max_payload (the per-FRAGMENT MTU budget). reassembly_budget is the
    // always-on aggregate reassembly-memory cap. Both required-WITH-default (the shipped
    // node-options constants); a transport stamps the topic-or-node effective-max here.
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
            // Floor the back-pressure cap at the per-message ceiling: the negotiated ceiling is
            // the sole size authority, so a single within-ceiling message's full fragment backlog
            // must always be admissible no matter how the operator tuned the back-pressure knob.
            // The knob only raises the cap above the floor to allow EXTRA backlog past one message.
            , m_backpressure(std::max(backpressure_bytes, max_message_bytes))
            , m_mode(mode)
            , m_initial_seq(initial_seq)
    {
    }

    udp_channel(const udp_channel &)            = delete;
    udp_channel &operator=(const udp_channel &) = delete;
    udp_channel(udp_channel &&)                 = delete;
    udp_channel &operator=(udp_channel &&)      = delete;

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
        if(m_reassembler)
            m_reassembler->cancel();
        if(m_arq)
            m_arq->cancel();
        if(m_on_teardown)
            m_on_teardown(); // erase the transport demux ref BEFORE this object dies
    }

    // The single byte_channel send verb. A reliable_datagram-mode channel (the "udpr"
    // route) dispatches to the in-order ARQ; a best_effort-mode channel (the "udp" route)
    // is fire-and-forget. This is how the erased polymorphic_byte_channel — which exposes only
    // send() — engages the ARQ on the flipped "udpr" route without a separate reliable verb.
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
        wire::wrap_udp_into(m_send_scratch, wire::udp_envelope_kind::best_effort, m_out_seq++, frame);
        m_server.send_standalone_to(m_send_scratch,
                                    m_dest); // a whole single datagram: idle fast-path eligible
    }

    void close()
    {
        if(!m_open)
            return;
        m_open = false;
        ::asio::post(m_io,
                     [this]
                     {
                         if(m_on_closed)
                             m_on_closed();
                     }); // posted, never synchronous
    }

    // The scheme reflects the channel's mode so a route is provable end-to-end: a
    // best_effort channel reports "udp", a reliable_datagram channel reports "udpr". This
    // lets the mux's "udpr" -> UDP+ARQ flip be test-pinned (the erased channel reports
    // "udpr", proving it rode the datagram member in reliable mode, NOT the TCP stream).
    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        const char *scheme = m_mode == datagram::detail::udp_channel_mode::reliable_datagram ? "udpr" : "udp";
        return {scheme, m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
    }

    [[nodiscard]] datagram::detail::udp_channel_mode mode() const noexcept
    {
        return m_mode;
    }

    // The negotiated per-session ISN (RFC 6528) this channel's receiver expects as its
    // first in-order seq; 0 on the legacy back-compat default. Behavior-only — it exposes
    // the value already bound at construction so a caller can reason about which seqs sit
    // below the receive window (a seq strictly below this is a provable duplicate).
    [[nodiscard]] std::uint16_t initial_seq() const noexcept
    {
        return m_initial_seq;
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    // The drop-observability seam (null by default — zero cost when unobserved). The owner
    // installs the engine's posted drop_sink; an ARQ shed at the publisher emits here, and
    // the lazily-built reassembler's own drop sink is forwarded onto this one so a
    // malformed/over-cap/timed-out fragment surfaces through the same edge. The sink POSTS,
    // so neither the shed site nor a reassembler fragment fires the observer synchronously.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop = std::move(cb);
    }

    // The transport's private teardown seam, fired from the dtor — distinct from the
    // consumer-facing on_closed/on_error the engine claims. The transport demuxes inbound
    // by endpoint to a NON-owning raw ref; this lets it erase that ref when the engine
    // (the channel's owner) destroys the channel, so a later datagram is a clean MISS
    // rather than a freed-pointer deref.
    void on_teardown(plexus::detail::move_only_function<void()> cb)
    {
        m_on_teardown = std::move(cb);
    }

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
            return detail::send_reliable_large(*this, payload);
        detail::ensure_arq(*this);
        const auto r = m_arq->submit(payload);
        if(r == submit_result::admitted)
            return r;
        return detail::on_window_full(*this, payload, /*fragmented=*/false);
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
    // Called BY the transport demux on each datagram for this peer (the channel owns no socket).
    // The strip/dedup/route body is relocated to detail::deliver_inbound.
    void deliver_inbound(std::span<const std::byte> datagram)
    {
        detail::deliver_inbound(*this, datagram);
    }

    // The stable per-construction id the egress scheduler keys its band map on (read via a
    // capability probe): unique per object, so a reused heap address cannot bleed a stale
    // band entry across — the same minted-once key every byte channel carries.
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    [[nodiscard]] const ::asio::ip::udp::endpoint &dest() const noexcept
    {
        return m_dest;
    }
    [[nodiscard]] bool is_open() const noexcept
    {
        return m_open;
    }
    [[nodiscard]] io::congestion congestion_mode() const noexcept
    {
        return m_congestion;
    }
    // The count of frames shed under congestion=drop (the future drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept
    {
        return m_dropped;
    }
    // The current backpressure-queue BYTE occupancy (congestion=block); 0 when the window
    // drains. Byte-valued (not the frame count) so it shares the stream channel's occupancy
    // contract and the egress scheduler's byte-denominated low-water gate compares like with like.
    [[nodiscard]] std::size_t backpressured() const noexcept
    {
        return m_backpressure.queued_bytes();
    }
    // The backpressure-queue byte cap, read by the egress scheduler so its low-water gate tracks
    // THIS channel's actual bound (lockstep with the channel's own admission).
    [[nodiscard]] std::size_t write_queue_capacity() const noexcept
    {
        return m_backpressure.capacity();
    }

private:
    // The send/recv path + reliable-ARQ glue is relocated to detail/udp_channel_io.h (relocation
    // by friendship): each helper reaches the members below through the channel reference.
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

    ::asio::io_context                                                                 &m_io;
    udp_server                                                                         &m_server;
    ::asio::ip::udp::endpoint                                                           m_dest;
    std::size_t                                                                         m_max_payload;        // per-FRAGMENT MTU budget (NOT the message ceiling)
    std::size_t                                                                         m_max_message_bytes;  // per-MESSAGE size ceiling (send + receive)
    std::size_t                                                                         m_reassembly_budget;  // aggregate reassembly-memory cap (always-on)
    std::chrono::milliseconds                                                           m_reassembly_timeout; // per-message reassembly reclaim window
    datagram::detail::udp_arq_config                                                    m_arq_cfg;
    io::congestion                                                                      m_congestion;
    datagram::detail::udp_backpressure_queue                                            m_backpressure; // bounded congestion=block queue
    std::size_t                                                                         m_dropped{0};   // congestion=drop shed count
    datagram::detail::udp_channel_mode                                                  m_mode;         // best_effort vs reliable_datagram
    std::uint16_t                                                                       m_initial_seq;  // negotiated per-session ISN (RFC 6528); 0 = legacy
    std::uint64_t                                                                       m_scheduler_key{io::detail::next_scheduler_key()}; // stable per-construction egress key
    std::uint16_t                                                                       m_out_seq{0};
    std::uint16_t                                                                       m_out_msg_id{0}; // per-message fragment grouping id (sender)
    wire::udp_dedup_window                                                              m_dedup;
    std::vector<std::byte>                                                              m_send_scratch;
    std::vector<std::byte>                                                              m_ack_scratch;
    std::vector<std::byte>                                                              m_arq_inner;
    std::vector<std::byte>                                                              m_frag_scratch; // reused fragment-encode buffer (allocated at setup)
    std::unique_ptr<arq_type>                                                           m_arq;
    std::unique_ptr<reassembler_type>                                                   m_reassembler;
    plexus::detail::move_only_function<void(std::span<const std::byte>)>                m_on_data;
    plexus::detail::move_only_function<void()>                                          m_on_closed;
    plexus::detail::move_only_function<void()>                                          m_on_teardown;
    plexus::detail::move_only_function<void(io::io_error)>                              m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)>                         m_on_protocol_close;
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_reliable;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)>            m_on_drop;
    bool                                                                                m_open{true};
};

}

static_assert(plexus::io::byte_channel<plexus::asio::udp_channel>,
              "udp_channel must satisfy byte_channel WITHOUT reshaping the concept — the "
              "NON-stream D2 proof");

#endif
