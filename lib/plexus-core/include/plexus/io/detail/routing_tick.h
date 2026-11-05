#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_TICK_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_TICK_H

#include "plexus/io/disconnect_cause.h"

#include "plexus/io/detail/routing_relay_emit.h"
#include "plexus/io/detail/routing_graph_change.h"
#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/node_id.h"

#include <chrono>
#include <vector>
#include <cstdint>

namespace plexus::io::detail {

// The keys are gathered first and torn down after the walk, since a teardown fires callbacks that may change the slots.
template<typename Registry>
void sweep_stale_sessions(Registry &reg, std::chrono::nanoseconds timeout)
{
    using session_type = typename Registry::session_type;

    if(timeout.count() == 0)
        return;
    std::vector<node_id> stale;
    reg.for_each_connected(
            [&](const node_id &id, const session_type &s)
            {
                if(detail::session_silent(s, timeout))
                    stale.push_back(id);
            });
    for(const node_id &id : stale)
        reg.tear_down_stale(id, disconnect_cause::heartbeat_timeout);
}

// The single per-tick action, not a second timer. The verdicts settle last so a TTL expiry or a stale teardown
// lands in the same tick's evaluation.
template<typename Engine>
void engine_tick(Engine &e, std::uint64_t &last_emit_ns)
{
    using session_type = typename Engine::session_type;

    const std::uint64_t now                = e.now_for_aging();
    const std::uint64_t interval           = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(e.m_liveliness.heartbeat_interval).count());
    const std::chrono::nanoseconds timeout = e.m_liveliness.session_teardown_timeout;
    // A tick whose own heartbeat is overdue by the teardown timeout ran late: the silence it would read is a local stall.
    const bool stalled = now - last_emit_ns >= static_cast<std::uint64_t>(timeout.count());
    if(now - last_emit_ns >= interval)
    {
        e.m_registry.for_each_connected([](const node_id &, session_type &s) { s.emit_heartbeat(); });
        detail::relay_reassert(e);
        last_emit_ns = now;
    }
    detail::sweep_aged_awareness(e);
    if(!stalled)
        detail::sweep_stale_sessions(e.m_registry, timeout);
    e.m_peer_liveliness.evaluate(now);
}

}

#endif
