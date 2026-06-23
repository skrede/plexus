#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_TRANSPORT_ACCEPT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_TRANSPORT_ACCEPT_H

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <memory>
#include <utility>

namespace plexus::tls::detail {

// The connect / handshake-pump + dial-resolution glue for tls_transport, relocated by friendship:
// each helper reaches the listener/credential/pending-dial members through the transport
// reference. Names carry a tls_ prefix where a bare name would collide with dtls_transport's glue.

template<typename T>
void tls_report_dial_fail(T &t, const io::endpoint &ep, io::io_error e)
{
    if(t.m_on_dial_failed)
        t.m_on_dial_failed(ep, e);
}

// The handshake succeeded: resolve the channel OUT of the registry and deliver it. Copy ep before
// resolve()/erase (ep is bound to the readiness closure capture the erase destroys). The consumer
// re-wires on_error when it adopts the channel from on_dialed.
template<typename T>
void tls_resolve_dial(T &t, const io::endpoint &ep, tls_channel *raw)
{
    const io::endpoint dialed = ep;
    auto               ch     = t.m_pending.resolve(raw);
    if(ch && t.m_on_dialed)
        t.m_on_dialed(std::move(ch), dialed);
}

// A handshake/verify failure: drop the dial through the registry's deferred-destroy (the channel
// is freed OFF its own async stack), then report on_dial_failed. Copy ep before fail()/erase.
template<typename T>
void tls_fail_dial(T &t, const io::endpoint &ep, tls_channel *raw, io::io_error e)
{
    const io::endpoint failed = ep;
    t.m_pending.fail(raw);
    tls_report_dial_fail(t, failed, e);
}

// Run the client handshake; deliver to on_dialed only on success. The minted channel is owned by
// the registry across the handshake (no self-owning readiness closure); a handshake/verify failure
// routes through the channel's on_error to tls_fail_dial (deferred-destroy + on_dial_failed).
template<typename T>
void tls_run_handshake(T &t, std::unique_ptr<tls_channel> ch, tls_channel *raw,
                       const io::endpoint &ep)
{
    t.m_pending.insert(raw, std::move(ch));
    raw->on_error([&t, ep, raw](io::io_error e) { tls_fail_dial(t, ep, raw, e); });
    auto host = sni_host(ep.address);
    raw->start_client_handshake(host, [&t, ep, raw]() mutable { tls_resolve_dial(t, ep, raw); });
}

}

#endif
