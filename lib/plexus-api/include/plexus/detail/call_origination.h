#ifndef HPP_GUARD_PLEXUS_DETAIL_CALL_ORIGINATION_H
#define HPP_GUARD_PLEXUS_DETAIL_CALL_ORIGINATION_H

#include "plexus/node_id.h"
#include "plexus/publisher_gid.h"

#include "plexus/wire/rpc_status.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <utility>
#include <optional>
#include <string>
#include <string_view>

namespace plexus::detail {

// The first reported via-relay origin — a non-direct route candidate whose reachability is a relay
// hop — excluding self. There is no served-procedure signal to match on (it is not propagated), so
// selection is over reachability only; the directory-free model cannot rank among several, so
// enumeration order decides.
template<typename Engine>
std::optional<node_id> first_via_relay_origin(const Engine &engine, const node_id &self)
{
    std::optional<node_id> chosen;
    engine.known().for_each_candidate([&](const node_id &id, const auto &reach, const auto &)
                                      { if(!chosen && reach.via.has_value() && id != self) chosen = id; });
    return chosen;
}

// Re-issue the caller ONCE through the engine's via-relay forwarder, honoring the caller deadline and
// adapting the (status, bytes) answer to OnReply with the origin as provider. Single-shot: the
// forwarded answer — success, unserved, or timeout — is delivered straight to the user, never re-tried.
template<typename Engine, typename OnReply>
void reissue_via_relay(Engine &engine, const node_id &origin, std::string_view fqn, std::span<const std::byte> param, OnReply on_reply,
                       std::optional<std::chrono::nanoseconds> deadline)
{
    const std::optional<publisher_gid> provider{publisher_gid{origin, 0}};
    engine.call(origin, fqn, param,
                [on_reply = std::move(on_reply), provider](wire::rpc_status status, std::span<const std::byte> bytes) mutable
                { on_reply(status, bytes, provider); },
                deadline);
}

// The direct call's response wrapper: on any answer BUT rpc_status::no_handler it delivers the direct
// (status, bytes, provider) unchanged; on no_handler it resolves a via-relay origin and, if one exists,
// re-issues once as a forwarded request (otherwise the original no_handler surfaces unchanged). It owns
// the fqn AND param — both view the caller's storage, which need not outlive the direct answer.
template<typename Engine, typename OnReply>
auto direct_response_with_fallback(Engine &engine, const node_id &self, std::string_view fqn, std::span<const std::byte> param, OnReply on_reply,
                                   std::optional<publisher_gid> provider, std::optional<std::chrono::nanoseconds> deadline)
{
    return [&engine, self, fqn = std::string{fqn}, param = std::vector<std::byte>{param.begin(), param.end()}, on_reply = std::move(on_reply), provider,
            deadline](wire::rpc_status status, std::span<const std::byte> bytes) mutable
    {
        if(status != wire::rpc_status::no_handler)
            return on_reply(status, bytes, provider);
        const std::optional<node_id> origin = first_via_relay_origin(engine, self);
        if(!origin)
            return on_reply(status, bytes, provider);
        reissue_via_relay(engine, *origin, fqn, param, std::move(on_reply), deadline);
    };
}

// Issue the plain direct call to a complete-session peer. When a via-relay origin exists the response
// is wrapped for the ex-post fallback; a pure-direct node (no non-direct candidate) pays nothing — it
// installs the byte-identical thin adapter and none of the ownership work.
template<typename Engine, typename Session, typename OnReply>
void issue_direct_call(Engine &engine, const node_id &self, Session &session, std::string_view fqn, std::span<const std::byte> param, OnReply on_reply,
                       std::optional<publisher_gid> provider, std::optional<std::chrono::nanoseconds> deadline, bool with_fallback)
{
    if(with_fallback)
        return engine.procedures().call(session.rpc_peer(), fqn, param,
                                        direct_response_with_fallback(engine, self, fqn, param, std::move(on_reply), provider, deadline), deadline,
                                        session.session_id());
    engine.procedures().call(session.rpc_peer(), fqn, param,
                             [on_reply = std::move(on_reply), provider](wire::rpc_status status, std::span<const std::byte> bytes) mutable
                             { on_reply(status, bytes, provider); },
                             deadline, session.session_id());
}

}

#endif
