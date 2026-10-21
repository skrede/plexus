#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_TRANSPORT_ACCEPT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_TRANSPORT_ACCEPT_H

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <asio/ip/tcp.hpp>

#include <string>
#include <utility>
#include <system_error>

namespace plexus::tls::detail {

// The connect / handshake-pump + dial-resolution glue for tls_transport: each helper reaches the
// listener/credential/pending-dial members through the transport reference. Names carry a tls_
// prefix where a bare name would collide with dtls_transport's glue.

template<typename T>
void tls_report_dial_fail(T &t, const io::endpoint &ep, io::io_error e)
{
    if(t.m_on_dial_failed_cb)
        t.m_on_dial_failed_cb(ep, e);
}

// The handshake succeeded: resolve the channel OUT of the registry and deliver it. Copy ep before
// resolve()/erase (ep is bound to the readiness closure capture the erase destroys). The consumer
// re-wires on_error when it adopts the channel from on_dialed.
template<typename T>
void tls_resolve_dial(T &t, const io::endpoint &ep, tls_channel *raw)
{
    const io::endpoint dialed = ep;
    t.m_pending.deliver_or_retire(t.m_on_dialed_cb, t.m_pending.resolve(raw), dialed);
}

// A handshake/verify failure: retire the dial in the registry, then report on_dial_failed. Copy ep
// before fail() erases the entry.
template<typename T>
void tls_fail_dial(T &t, const io::endpoint &ep, tls_channel *raw, io::io_error e)
{
    const io::endpoint failed = ep;
    t.m_pending.fail(raw);
    tls_report_dial_fail(t, failed, e);
}

// Run the client handshake; deliver to on_dialed only on success. The dial's registry entry, made before its connect,
// holds the channel across the handshake; a handshake/verify failure routes through the channel's on_error to
// tls_fail_dial.
template<typename T>
void tls_run_handshake(T &t, tls_channel *raw, const io::endpoint &ep)
{
    raw->on_error([&t, ep, raw](io::io_error e) { tls_fail_dial(t, ep, raw, e); });
    auto host = sni_host(ep.address);
    raw->start_client_handshake(host, [&t, ep, raw]() mutable { tls_resolve_dial(t, ep, raw); });
}

// A connect that succeeded for a dial close() already retired is still that dial's one failure.
template<typename T>
void tls_finish_connect(T &t, const io::endpoint &ep, tls_channel *raw, std::error_code ec)
{
    raw->end_op();
    if(ec)
        return tls_fail_dial(t, ep, raw, plexus::asio::detail::map_error(ec));
    if(!t.m_pending.is_pending(raw))
        return tls_report_dial_fail(t, ep, io::io_error::operation_aborted);
    if(t.m_no_delay)
    {
        std::error_code nec;
        (void)raw->socket().set_option(::asio::ip::tcp::no_delay(true), nec);
    }
    tls_run_handshake(t, raw, ep);
}

}

#endif
