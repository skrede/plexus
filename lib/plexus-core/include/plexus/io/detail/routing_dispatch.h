#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_DISPATCH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_DISPATCH_H

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/security_event.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <span>
#include <vector>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace plexus::io::detail {

// The posted observer fan-out discipline, factored out of the engine body. Every edge is POSTED on
// the BORROWED executor over a SNAPSHOT of the observer list: a callback that (un)registers an
// observer mutates the live list without invalidating the in-flight iteration — a same-turn remove
// is honored, a same-turn add takes effect on the next posted edge. RELOCATION: the engine owns
// m_observers/m_executor; these read them through it (they are friends).

// Skip any observer unregistered mid-turn so a callback may safely (un)register without corrupting
// the fan-out.
template<typename Engine, typename Deliver>
void fan_out(Engine &e, Deliver deliver)
{
    const auto snapshot = e.m_observers;
    for(observer &o : snapshot)
        if(std::any_of(e.m_observers.begin(), e.m_observers.end(), [&](const std::reference_wrapper<observer> &w) { return &w.get() == &o; }))
            deliver(o);
}

template<typename Engine>
void post_edge(Engine &e, const lifecycle_event &ev, void (observer::*edge)(const node_id &, std::string_view, peer_kind))
{
    using policy_type = typename Engine::policy_type;
    policy_type::post(e.m_executor, [&e, ev, edge] { fan_out(e, [&](observer &o) { (o.*edge)(ev.id, ev.node_name, ev.kind); }); });
}

template<typename Engine>
void post_rejected(Engine &e, const lifecycle_event &ev)
{
    using policy_type = typename Engine::policy_type;
    policy_type::post(e.m_executor, [&e, ev] { fan_out(e, [&](observer &o) { o.on_peer_rejected(ev.id, ev.node_name, ev.reason); }); });
}

// The indirection that keeps a per-packet drop site off the synchronous observer path.
template<typename Engine>
void post_drop(Engine &e, const drop_event &ev)
{
    using policy_type = typename Engine::policy_type;
    policy_type::post(e.m_executor, [&e, ev] { fan_out(e, [&](observer &o) { o.on_drop(ev); }); });
}

// Never inline from the session's teardown/refusal frame.
template<typename Engine>
void post_security(Engine &e, const security_event &ev)
{
    using policy_type = typename Engine::policy_type;
    policy_type::post(e.m_executor, [&e, ev] { fan_out(e, [&](observer &o) { o.on_security(ev); }); });
}

// Keeps the single-writer ring discipline (push only on the executor turn, never the io thread).
// The span into io-thread-owned bytes does NOT survive the post, so the frame is COPIED into an
// owned buffer carried BY MOVE into the turn and the span rebuilt over it. This copy-into-turn is
// the inherent every-packet cost of wire capture.
template<typename Engine>
void post_wire(Engine &e, recording::wire_direction dir, std::uint64_t seq, const node_id &peer, std::span<const std::byte> bytes)
{
    using policy_type = typename Engine::policy_type;
    std::vector<std::byte> owned(bytes.begin(), bytes.end());
    policy_type::post(e.m_executor,
                      [&e, dir, seq, peer, owned = std::move(owned)]
                      {
                          const recording::wire_record rec{dir, seq, peer, 0u, std::span<const std::byte>{owned}};
                          fan_out(e, [&](observer &o) { o.on_wire(rec); });
                      });
}

}

#endif
