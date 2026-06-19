#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_IO_H

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"
#include "plexus/wire/udp_dedup_window.h"

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"

#include <asio/post.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::asio::detail {

// over-limit: one cohesive UDP I/O state machine; the send-large/fragment, the inbound
// demux/dedup/reassembly, the ARQ build/fan, and the backpressure drain all advance the shared
// channel members (the lazily-built ARQ + reassembler, the scratch buffers, the bounded queue),
// so splitting them further scatters that shared I/O state with no readability gain.

// The send/recv path + reliable-ARQ glue for udp_channel, relocated by friendship: each helper
// reaches the channel's server/dest/scratch/ARQ/reassembler/backpressure members through the
// channel reference. The ARQ itself lives in io/detail/udp_reliable_arq.h; this carries the
// channel-side wiring (build, fan, drain) out of the channel header.

template<typename Ch>
void reject_oversize(Ch &c)
{
    if(c.m_on_error)
        c.m_on_error(io::io_error::message_too_large);
}

template<typename Ch>
[[nodiscard]] bool exceeds_max_message(const Ch &c, std::size_t size) noexcept
{
    return size > c.m_max_message_bytes;
}

// best_effort split: each fragment is one FRAGMENTED-bit envelope carrying the wire sub-header +
// slice, sent fire-and-forget. Any lost fragment leaves the message incomplete; the peer's
// per-message reassembly timeout drops the WHOLE message.
template<typename Ch>
void send_best_effort_large(Ch &c, std::span<const std::byte> frame)
{
    if(exceeds_max_message(c, frame.size()))
        return reject_oversize(c);
    const std::uint16_t msg_id = c.m_out_msg_id++;
    io::fragment_sink   sink =
            [&c, msg_id](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> slice)
    {
        wire::wrap_udp_fragment_into(c.m_frag_scratch, wire::udp_envelope_kind::best_effort,
                                     c.m_out_seq++, msg_id, idx, cnt, slice);
        c.m_server.send_to(c.m_frag_scratch, c.m_dest);
    };
    io::split(frame, c.m_max_payload, msg_id, sink);
}

// on_data is ALWAYS posted (the byte_channel contract). The owning vector keeps the bytes alive
// across the post (the demux's recv buffer is reused immediately).
template<typename Ch>
void post_on_data(Ch &c, std::span<const std::byte> frame)
{
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(c.m_io,
                 [&c, owned]
                 {
                     if(c.m_on_data)
                         c.m_on_data(std::span<const std::byte>{*owned});
                 });
}

// A reassembled message already owns its bytes once: carry the owner straight into the posted
// closure (no second copy) and hand its span up.
template<typename Ch>
void post_on_data_owned(Ch &c, wire::shared_bytes owned)
{
    ::asio::post(c.m_io,
                 [&c, owned = std::move(owned)]
                 {
                     if(c.m_on_data)
                         c.m_on_data(static_cast<std::span<const std::byte>>(owned));
                 });
}

// Build the bounded reassembler on first inbound fragment and wire its completion sink: an
// assembled message POSTS on_data (the block is sans-IO; the channel owns the post).
template<typename Ch>
void ensure_reassembler(Ch &c)
{
    if(c.m_reassembler)
        return;
    using reassembler_type = typename Ch::reassembler_type;
    c.m_reassembler        = std::make_unique<reassembler_type>(
            c.m_io,
            typename reassembler_type::config{.max_message_size    = c.m_max_message_bytes,
                                              .total_memory_cap    = c.m_reassembly_budget,
                                              .per_message_timeout = c.m_reassembly_timeout});
    c.m_reassembler->on_deliver([&c](wire::shared_bytes msg)
                                { post_on_data_owned(c, std::move(msg)); });
    c.m_reassembler->on_drop(
            [&c](const io::detail::drop_event &ev)
            {
                if(c.m_on_drop)
                    c.m_on_drop(ev);
            });
}

// A best_effort inbound fragment: decode its wire sub-header (fail-closed) and feed the bounded
// reassembler; a completed message posts on_data via on_deliver.
template<typename Ch>
void feed_fragment(Ch &c, std::span<const std::byte> frame)
{
    ensure_reassembler(c);
    auto h = wire::decode_udp_fragment_header(frame);
    if(!h)
        return; // malformed sub-header: drop
    c.m_reassembler->feed(h->msg_id, h->frag_idx, h->frag_cnt, h->payload);
}

// A reliable in-order payload: if its ARQ seq was flagged FRAGMENTED, the payload is
// [msg_id:2][idx:4][cnt:4][slice] — decode and feed the reassembler; else it is a whole reliable
// message delivered byte-identically to the unfragmented path.
template<typename Ch>
void deliver_reliable_inorder(Ch &c, bool fragmented, std::span<const std::byte> payload)
{
    if(!fragmented)
        return post_on_data(c, payload); // whole reliable message: unchanged path
    ensure_reassembler(c);
    auto h = wire::decode_udp_fragment_header(payload);
    if(!h)
        return; // malformed: drop the fragment
    c.m_reassembler->feed(h->msg_id, h->frag_idx, h->frag_cnt, h->payload);
}

// The per-connection congestion safety net for the direct-send bypass paths. congestion=block
// enqueues a window-full reliable frame into the bounded queue (the ack handler drains it); a
// queue at its cap surfaces would_block. drop_newest sheds the frame at the publisher. Either way
// publish() stays non-blocking.
template<typename Ch>
typename Ch::submit_result on_window_full(Ch &c, std::span<const std::byte> payload,
                                          bool fragmented)
{
    using sr = typename Ch::submit_result;
    if(c.m_congestion == io::congestion::drop_newest)
    {
        ++c.m_dropped;
        if(c.m_on_drop)
            c.m_on_drop(io::detail::drop_event{.cause     = io::detail::drop_cause::arq_shed,
                                               .transport = io::locality::remote});
        return sr::window_full;
    }
    if(!c.m_backpressure.admit(payload, fragmented))
    {
        if(c.m_on_error)
            c.m_on_error(io::io_error::would_block); // queue at cap: the stall edge
        return sr::window_full;
    }
    return sr::admitted; // accepted into the queue; will send on the next ack
}

// Encode one reliable fragment as [msg_id:2][idx:4][cnt:4][slice] and submit it to the ARQ with
// the fragmented flag set; a full window backpressures through the bounded queue.
template<typename Ch>
typename Ch::submit_result submit_reliable_fragment(Ch &c, std::uint16_t msg_id, std::uint32_t idx,
                                                    std::uint32_t              cnt,
                                                    std::span<const std::byte> slice)
{
    wire::encode_udp_fragment_payload_into(c.m_frag_scratch, msg_id, idx, cnt, slice);
    const auto r = c.m_arq->submit(c.m_frag_scratch, /*fragmented=*/true);
    if(r == Ch::submit_result::admitted)
        return r;
    return on_window_full(c, c.m_frag_scratch, /*fragmented=*/true);
}

// reliable split: each fragment is submitted as one send_reliable segment ABOVE the ARQ, so a
// lost fragment is selectively retransmitted and the message completes. The fragment's
// msg_id/index/count ride INSIDE the segment payload; the FRAGMENTED envelope bit tells the peer
// to route the in-order payload to the reassembler.
template<typename Ch>
typename Ch::submit_result send_reliable_large(Ch &c, std::span<const std::byte> payload)
{
    if(exceeds_max_message(c, payload.size()))
    {
        reject_oversize(c);
        return Ch::submit_result::window_full;
    }
    ensure_arq(c);
    const std::uint16_t        msg_id = c.m_out_msg_id++;
    typename Ch::submit_result last   = Ch::submit_result::admitted;
    io::fragment_sink          sink   = [&c, msg_id, &last](std::uint32_t idx, std::uint32_t cnt,
                                                            std::span<const std::byte> slice)
    { last = submit_reliable_fragment(c, msg_id, idx, cnt, slice); };
    io::split(payload, c.m_max_payload, msg_id, sink);
    return last;
}

// Re-submit queued frames while the send window has room: each admit pops one. A submit that
// returns window_full stops the drain — the remainder waits for the next on_window_advance.
template<typename Ch>
void drain_backpressure(Ch &c)
{
    while(!c.m_backpressure.empty() && c.m_arq && c.m_arq->window_has_room())
    {
        if(c.m_arq->submit(c.m_backpressure.front(), c.m_backpressure.front_fragmented()) !=
           Ch::submit_result::admitted)
            break;
        c.m_backpressure.pop_front();
    }
}

// Build the selective-repeat ARQ on first reliable use and wire its actions: a (re)transmit wraps
// a kind=1 data segment, an ack wraps a kind=1 ack frame, an in-order payload posts on_data, and
// exhaustion surfaces a connection-fatal error.
template<typename Ch>
void ensure_arq(Ch &c)
{
    if(c.m_arq)
        return;
    using arq_type = typename Ch::arq_type;
    c.m_arq        = std::make_unique<arq_type>(c.m_io, c.m_arq_cfg, c.m_initial_seq);
    c.m_arq->on_transmit(
            [&c](std::uint16_t seq, std::span<const std::byte> payload, bool fragmented)
            {
                wire::encode_udp_segment_into(c.m_arq_inner, payload);
                if(fragmented)
                    wire::wrap_udp_into_fragmented(c.m_send_scratch,
                                                   wire::udp_envelope_kind::reliable_arq, seq,
                                                   c.m_arq_inner);
                else
                    wire::wrap_udp_into(c.m_send_scratch, wire::udp_envelope_kind::reliable_arq,
                                        seq, c.m_arq_inner);
                c.m_server.send_to(c.m_send_scratch, c.m_dest);
            });
    c.m_arq->on_send_ack(
            [&c](const wire::udp_ack &ack)
            {
                wire::encode_udp_ack_into(c.m_arq_inner, ack);
                wire::wrap_udp_into(c.m_ack_scratch, wire::udp_envelope_kind::reliable_arq, 0,
                                    c.m_arq_inner);
                c.m_server.send_to(c.m_ack_scratch, c.m_dest);
            });
    c.m_arq->on_deliver_seq([&c](std::uint16_t, bool fragmented, std::span<const std::byte> payload)
                            { deliver_reliable_inorder(c, fragmented, payload); });
    c.m_arq->on_exhausted(
            [&c]
            {
                if(c.m_on_error)
                    c.m_on_error(io::io_error::timed_out);
            });
    c.m_arq->on_window_advance([&c] { drain_backpressure(c); });
}

// Fan a kind=1 inner frame to the ARQ on the ONE inbound demux path: a data segment drives
// on_segment (in-order delivery + ack), an ack drives on_ack (window slide). A test-installed raw
// observer (m_on_reliable) sees the segment too. A frame whose marker is neither is dropped.
template<typename Ch>
void deliver_reliable(Ch &c, std::uint16_t seq, bool fragmented, std::span<const std::byte> inner)
{
    auto kind = wire::peek_udp_arq_kind(inner);
    if(!kind)
        return;
    if(*kind == wire::udp_arq_kind::ack)
    {
        if(auto ack = wire::decode_udp_ack(inner); ack && c.m_arq)
            c.m_arq->on_ack(*ack);
        return;
    }
    auto payload = wire::decode_udp_segment(inner);
    if(!payload)
        return;
    if(c.m_on_reliable)
        c.m_on_reliable(seq, *payload);
    ensure_arq(c);
    c.m_arq->on_segment(seq, fragmented, *payload);
}

// Strip the envelope, dedup best_effort, route to the reassembler or post the inner frame. A
// kind=1 datagram engages the ARQ only on a reliable_datagram-mode channel (mode, not envelope
// kind alone, gates the engine — a spoofed datagram cannot spin up an unsolicited ARQ).
template<typename Ch>
void deliver_inbound(Ch &c, std::span<const std::byte> datagram)
{
    auto dec = wire::unwrap_udp(datagram);
    if(!dec)
        return; // malformed: drop, never on_protocol_close
    if(dec->kind == wire::udp_envelope_kind::best_effort)
    {
        if(c.m_dedup.admit(dec->seq) != wire::udp_dedup_window::outcome::fresh)
            return; // duplicate / too_old: drop
        if(dec->fragmented)
            return feed_fragment(c, dec->frame); // best_effort fragment -> reassembler
        post_on_data(c, dec->frame);
    }
    else if(c.m_mode == io::detail::udp_channel_mode::reliable_datagram)
        deliver_reliable(c, dec->seq, dec->fragmented, dec->frame);
}

}

#endif
