#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_RPC_SEAMS_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_RPC_SEAMS_H

#include "plexus/detail/fail_closed.h"
#include "plexus/detail/call_origination.h"

#include "plexus/publisher_gid.h"

#include "plexus/io/endpoint_seam.h"
#include "plexus/io/route_options.h"

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::detail {

// Routes to the first connection-order peer with a complete session and issues the plain direct
// call. The consumer has no served-fqn signal to pre-decide direct-vs-forward, so resolution is
// EX-POST: only when that direct answer is rpc_status::no_handler does the wrapper resolve a
// reported via-relay origin and re-issue the request ONCE as a forwarded call. With NO connected
// provider the completion is POSTED with an absent status (the out-of-band no_provider verdict,
// never a fabricated rpc_status) — it never hangs, buffers, or queues. A pure-direct node (no
// non-direct candidate) installs the byte-identical thin adapter and pays none of the fallback work.
template<typename Node, typename OnReply>
void call_seam(Node &n, std::string_view fqn, std::span<const std::byte> param, OnReply on_reply, std::optional<std::chrono::nanoseconds> deadline)
{
    const bool with_fallback = n.m_engine.route_opts().usage != io::route_usage::never && detail::first_via_relay_origin(n.m_engine, n.m_id).has_value();
    for(const auto &peer : n.m_known_peers)
    {
        auto *session = n.m_engine.registry().session_for(peer);
        if(session == nullptr || !session->is_complete())
            continue;
        const std::optional<publisher_gid> provider{publisher_gid{peer, 0}};
        detail::issue_direct_call(n.m_engine, n.m_id, *session, fqn, param, std::move(on_reply), provider, deadline, with_fallback);
        return;
    }
    Node::policy_type::post(n.m_executor, [on_reply = std::move(on_reply)]() mutable { on_reply(std::nullopt, {}, std::nullopt); });
}

// The served-FQN set is checked BEFORE the forwarder is touched, so a refused (duplicate-local)
// registration throws with ZERO side effects, closing the forwarder's silent-overwrite hijack.
template<typename Node>
void serve_procedure_seam(Node &n, std::string_view fqn, io::handler_fn handler)
{
    if(std::find(n.m_served_fqns.begin(), n.m_served_fqns.end(), fqn) != n.m_served_fqns.end())
        plexus::detail::fail_closed("plexus: a procedure is already served locally on this fqn");
    n.m_served_fqns.emplace_back(fqn);
    n.m_engine.procedures().serve(fqn, std::move(handler));
}

template<typename Node>
void retire_procedure_seam(Node &n, std::string_view fqn)
{
    n.m_engine.procedures().retire(fqn);
    std::erase(n.m_served_fqns, std::string{fqn});
}

}

#endif
