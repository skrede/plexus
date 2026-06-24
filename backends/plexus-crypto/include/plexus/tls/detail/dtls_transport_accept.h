#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_TRANSPORT_ACCEPT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_TRANSPORT_ACCEPT_H

#include "plexus/tls/dtls_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/detail/drop_event.h"

#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::tls::detail {

// The listener / accept / cookie-exchange + dial-resolution glue for dtls_transport: each helper
// reaches the server/demux/dial-table members through the transport reference. The OpenSSL
// handshake pump stays in dtls_channel.

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

template<typename T>
void ensure_bound(T &t, const ::asio::ip::udp &proto)
{
    if(!t.m_server.is_open())
        t.m_server.start(typename T::endpoint_type(proto, 0));
}

// The borrow-vs-own teardown seam: the demux keeps a non-owning ref, so the channel's dtor drops
// it (identity-guarded so a same-endpoint re-dial is untouched).
template<typename T>
void wire_teardown(T &t, dtls_channel &ch, const typename T::endpoint_type &key)
{
    ch.on_teardown([&t, key, raw = &ch] { t.m_demux.erase_if_matches(key, raw); });
}

// An accepted server channel completed its mutual handshake: hand it to on_accepted while the
// demux raw ref keeps routing inbound to it.
template<typename T>
void resolve_accept(T &t, dtls_channel *raw)
{
    auto ch = t.m_registry.adopt_accepted(raw);
    if(!ch)
        return;
    if(t.m_on_accepted_cb)
        t.m_on_accepted_cb(std::move(ch));
}

// A handshake/verify failure on an accept-side channel: drop it fail-closed (the source never
// proved a pinned identity). fail_accepted routes the freed channel through the deferred-destroy
// sink; detach the demux ref now (the failure fired from inside raw's own drain stack).
template<typename T>
void drop_accept(T &t, dtls_channel *raw)
{
    t.m_demux.erase(raw->dest());
    t.m_registry.fail_accepted(raw);
}

// A never-seen source: mint a server-side channel behind the cookie gate (OpenSSL drives
// HelloVerifyRequest — no full handshake state until the source echoes a valid cookie). The demux
// cap bounds the spoof-flood. The triggering ClientHello is fed AFTER minting; on_external_complete
// fires on_accepted post mutual-verify (fail-closed).
template<typename T>
void accept_new_peer(T &t, const typename T::endpoint_type &from, std::span<const std::byte> bytes)
{
    auto ch   = std::make_unique<dtls_channel>(t.m_io, t.m_server, from, t.m_cred, t.m_cookie, dtls_channel::role::server, t.m_max_payload, dtls_channel::default_record_mtu,
                                               t.m_max_message_bytes, t.m_reassembly_budget);
    auto *raw = ch.get();
    if(!t.m_demux.insert(from, raw))
    {
        if(t.m_on_drop_cb)
            t.m_on_drop_cb(io::detail::drop_event{.cause = io::detail::drop_cause::demux_refused, .transport = io::locality::remote});
        return; // peer cap reached: drop the flood
    }
    wire_teardown(t, *raw, from);
    t.m_registry.insert_accepted(raw, std::move(ch));
    raw->on_external_complete([&t, raw] { resolve_accept(t, raw); });
    raw->on_error([&t, raw](io::io_error) { drop_accept(t, raw); });
    raw->start_handshake();
    raw->deliver_inbound(bytes); // feed the triggering ClientHello
}

template<typename T>
void on_datagram(T &t, const typename T::endpoint_type &from, std::span<const std::byte> bytes)
{
    if(auto *ch = t.m_demux.lookup(from))
        return ch->deliver_inbound(bytes);
    accept_new_peer(t, from, bytes);
}

// COPY ep before resolve erases the entry (ep is bound to the pending entry's closure capture; the
// erase destroys it — re-emitting the freed reference is a use-after-free).
template<typename T>
void resolve_dial(T &t, const io::endpoint &ep, dtls_channel *raw)
{
    const io::endpoint dialed = ep;
    auto ch                   = t.m_registry.resolve(raw);
    if(!ch)
        return;
    if(t.m_on_dialed_cb)
        t.m_on_dialed_cb(std::move(ch), dialed);
}

// COPY ep AND erase the demux ref before fail() moves the channel out (fail() fired from inside
// raw's own drain stack, so the registry routes the freed channel through the deferred-destroy
// sink). Thread the channel's actual error through so the consumer can distinguish "peer never
// answered" from "peer presented an unpinned cert".
template<typename T>
void fail_dial(T &t, const io::endpoint &ep, dtls_channel *raw, io::io_error e)
{
    const io::endpoint failed = ep;
    t.m_demux.erase(raw->dest());
    t.m_registry.fail(raw);
    report_dial_fail(t, failed, e);
}

}

#endif
