#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_TICK_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_TICK_H

#include "plexus/io/detail/routing_relay_emit.h"
#include "plexus/io/detail/routing_graph_change.h"

#include "plexus/node_id.h"

#include <chrono>
#include <cstdint>

namespace plexus::io::detail {

// The single per-tick action (NOT a second timer): emit a heartbeat to every connected peer when
// the configured interval has elapsed (the default equals the tick cadence, so emission is
// per-tick and wire-identical) and, on the same cadence, re-assert every held relay report so a
// still-live-but-idle reported origin refreshes downstream before it ages out at awareness_ttl
// (a relay-only no-op on a non-relay node); then sweep the awareness table for peers past their
// discovery TTL and settle the fused verdicts so a TTL expiry lands in the same tick's evaluation.
template<typename Engine>
void engine_tick(Engine &e, std::uint64_t &last_emit_ns)
{
    using session_type = typename Engine::session_type;

    const std::uint64_t now      = e.now_for_aging();
    const std::uint64_t interval = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(e.m_liveliness.heartbeat_interval).count());
    if(now - last_emit_ns >= interval)
    {
        e.m_registry.for_each_connected([](const node_id &, session_type &s) { s.emit_heartbeat(); });
        detail::relay_reassert(e);
        last_emit_ns = now;
    }
    detail::sweep_aged_awareness(e);
    e.m_peer_liveliness.evaluate(now);
}

}

#endif
