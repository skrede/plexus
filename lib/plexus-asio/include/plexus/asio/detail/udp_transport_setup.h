#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_TRANSPORT_SETUP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_TRANSPORT_SETUP_H

#include "plexus/asio/udp_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/udp_handshake_frame.h"

#include <asio/post.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::asio::detail {

// The socket-setup + accept-loop + dial-resolution glue for udp_transport, relocated by
// friendship: each helper reaches the transport's server/demux/dial-table members through the
// transport reference. The per-peer handshake ARQ stays in io/detail/udp_handshake_arq.h.

template<typename T>
void report_dial_fail(T &t, const io::endpoint &ep, io::io_error e)
{
    if(t.m_on_dial_failed)
        t.m_on_dial_failed(ep, e);
}

template<typename T>
void report_error(T &t, io::io_error e)
{
    if(t.m_on_error)
        t.m_on_error(e);
}

// Bind the shared socket to an ephemeral local endpoint if listen() has not already bound it: a
// dial-only transport still needs a bound source port to send from and receive replies on.
template<typename T>
void ensure_bound(T &t, const ::asio::ip::udp &proto)
{
    if(!t.m_server.is_open())
        t.m_server.start(typename T::endpoint_type(proto, 0));
}

template<typename T>
void send_handshake(T &t, const typename T::endpoint_type &dest, typename T::hs_type type,
                    io::detail::udp_channel_mode mode = io::detail::udp_channel_mode::best_effort,
                    std::uint16_t                initial_seq = 0)
{
    io::detail::encode_handshake_into(t.m_hs_scratch, type, mode, initial_seq);
    t.m_server.send_to(t.m_hs_scratch, dest);
}

// The borrow-vs-own teardown seam: the engine owns the handed-out channel but the demux keeps a
// non-owning ref, so the channel's dtor must drop that ref. The identity-guarded erase leaves a
// same-endpoint re-dial untouched.
template<typename T>
void wire_teardown(T &t, udp_channel &ch, const typename T::endpoint_type &key)
{
    ch.on_teardown([&t, key, raw = &ch] { t.m_demux.erase_if_matches(key, raw); });
}

template<typename T>
void resolve_paired(T &t, udp_channel *ch)
{
    if(auto *arq = t.m_dials.payload_of(ch))
        (*arq)->on_paired_frame();
}

// COPY ep before resolve erases the entry: ep is bound to the pending entry's ARQ-closure
// capture, which the erase destroys — re-emitting the freed reference is a use-after-free.
template<typename T>
void resolve_dial(T &t, const io::endpoint &ep, udp_channel *raw)
{
    const io::endpoint dialed = ep;
    auto               ch     = t.m_dials.resolve(raw);
    if(!ch)
        return;
    if(t.m_on_dialed)
        t.m_on_dialed(std::move(ch), dialed);
}

// COPY ep AND erase the demux entry out before fail erases the registry entry (the entry owns the
// ARQ closure ep is bound to, and the channel whose dest the demux is keyed on is moved out).
template<typename T>
void fail_dial(T &t, const io::endpoint &ep, udp_channel *raw)
{
    const io::endpoint failed = ep;
    t.m_demux.erase(raw->dest());
    t.m_dials.fail(raw); // routes the freed channel through the deferred-destroy sink
    report_dial_fail(t, failed, io::io_error::timed_out);
}

// A never-seen source: ONLY a handshake request synthesizes an accept (the source endpoint is not
// trusted as identity — bare data is dropped; the demux cap bounds the spoof-flood). The
// dialer-declared mode mints a symmetric channel and is echoed in the response.
template<typename T>
void accept_new_peer(T &t, const typename T::endpoint_type &from, std::span<const std::byte> bytes)
{
    auto hs = io::detail::decode_handshake(bytes);
    if(!hs || hs->type != T::hs_type::request)
        return;
    auto  ch = std::make_unique<udp_channel>(t.m_io, t.m_server, from, t.m_max_payload, t.m_arq_cfg,
                                             t.m_congestion, t.m_backpressure_bytes, hs->mode,
                                             hs->initial_seq, t.m_global_default,
                                             t.m_reassembly_budget, t.m_reassembly_timeout);
    auto *raw = ch.get();
    if(!t.m_demux.insert(from, raw))
    {
        if(t.m_on_drop)
            t.m_on_drop(io::detail::drop_event{.cause     = io::detail::drop_cause::demux_refused,
                                               .transport = io::locality::remote});
        return; // peer cap reached: drop the flood
    }
    wire_teardown(t, *raw, from);
    t.m_dials.insert_accepted(raw, std::move(ch));
    send_handshake(t, from, T::hs_type::response, hs->mode, hs->initial_seq);
    if(t.m_on_accepted)
        t.m_on_accepted(t.m_dials.adopt_accepted(raw));
}

// A known peer: a handshake control frame drives the ARQ / replies; anything else is data the
// channel deduplicates and posts.
template<typename T>
void route_to_peer(T &t, const typename T::endpoint_type &from, udp_channel *ch,
                   std::span<const std::byte> bytes)
{
    if(auto hs = io::detail::decode_handshake(bytes))
    {
        if(hs->type == T::hs_type::response)
            resolve_paired(t, ch);
        else
            send_handshake(t, from, T::hs_type::response, hs->mode, hs->initial_seq);
        return;
    }
    ch->deliver_inbound(bytes);
}

template<typename T>
void on_datagram(T &t, const typename T::endpoint_type &from, std::span<const std::byte> bytes)
{
    if(auto *ch = t.m_demux.lookup(from))
        return route_to_peer(t, from, ch, bytes);
    accept_new_peer(t, from, bytes);
}

}

#endif
