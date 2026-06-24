#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_TRANSPORT_SETUP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_TRANSPORT_SETUP_H

#include "plexus/asio/udp_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"

#include <asio/post.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::asio::detail {

template<typename T>
void report_dial_fail(T &t, const io::endpoint &ep, io::io_error e)
{
    if(t.m_on_dial_failed_cb)
        t.m_on_dial_failed_cb(ep, e);
}

template<typename T>
void report_error(T &t, io::io_error e)
{
    if(t.m_on_error_cb)
        t.m_on_error_cb(e);
}

// A dial-only transport still needs a bound source port, so bind an ephemeral local endpoint if
// listen() has not already bound the shared socket.
template<typename T>
void ensure_bound(T &t, const ::asio::ip::udp &proto)
{
    if(!t.m_server.is_open())
        t.m_server.start(typename T::endpoint_type(proto, 0));
}

template<typename T>
void send_handshake(T &t, const typename T::endpoint_type &dest, typename T::hs_type type, datagram::detail::udp_channel_mode mode = datagram::detail::udp_channel_mode::best_effort,
                    std::uint16_t initial_seq = 0)
{
    datagram::detail::encode_handshake_into(t.m_hs_scratch, type, mode, initial_seq);
    t.m_server.send_to(t.m_hs_scratch, dest);
}

// The engine owns the handed-out channel but the demux keeps a non-owning ref, so the channel's
// dtor drops it; the identity-guarded erase leaves a same-endpoint re-dial untouched.
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

// COPY ep before resolve erases the entry: ep is bound to the pending entry's ARQ-closure capture
// that the erase destroys, so re-emitting the reference would be a use-after-free.
template<typename T>
void resolve_dial(T &t, const io::endpoint &ep, udp_channel *raw)
{
    const io::endpoint dialed = ep;
    auto ch                   = t.m_dials.resolve(raw);
    if(!ch)
        return;
    if(t.m_on_dialed_cb)
        t.m_on_dialed_cb(std::move(ch), dialed);
}

// COPY ep and erase the demux entry before fail erases the registry entry (which owns the ARQ
// closure ep is bound to, and moves out the channel the demux is keyed on).
template<typename T>
void fail_dial(T &t, const io::endpoint &ep, udp_channel *raw)
{
    const io::endpoint failed = ep;
    t.m_demux.erase(raw->dest());
    t.m_dials.fail(raw); // routes the freed channel through the deferred-destroy sink
    report_dial_fail(t, failed, io::io_error::timed_out);
}

// ONLY a handshake request synthesizes an accept from a never-seen source (bare data is not
// trusted as identity); the demux cap bounds the spoof-flood.
template<typename T>
void accept_new_peer(T &t, const typename T::endpoint_type &from, std::span<const std::byte> bytes)
{
    auto hs = datagram::detail::decode_handshake(bytes);
    if(!hs || hs->type != T::hs_type::request)
        return;
    auto ch   = std::make_unique<udp_channel>(t.m_io, t.m_server, from, t.m_max_payload, t.m_arq_cfg, t.m_congestion, t.m_backpressure_bytes, hs->mode, hs->initial_seq,
                                              t.m_global_default, t.m_reassembly_budget, t.m_reassembly_timeout);
    auto *raw = ch.get();
    if(!t.m_demux.insert(from, raw))
    {
        if(t.m_on_drop_cb)
            t.m_on_drop_cb(io::detail::drop_event{.cause = io::detail::drop_cause::demux_refused, .transport = io::locality::remote});
        return; // peer cap reached: drop the flood
    }
    wire_teardown(t, *raw, from);
    t.m_dials.insert_accepted(raw, std::move(ch));
    send_handshake(t, from, T::hs_type::response, hs->mode, hs->initial_seq);
    if(t.m_on_accepted_cb)
        t.m_on_accepted_cb(t.m_dials.adopt_accepted(raw));
}

template<typename T>
void route_to_peer(T &t, const typename T::endpoint_type &from, udp_channel *ch, std::span<const std::byte> bytes)
{
    if(auto hs = datagram::detail::decode_handshake(bytes))
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
