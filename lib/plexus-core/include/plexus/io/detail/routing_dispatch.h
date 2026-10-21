#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_DISPATCH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_DISPATCH_H

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/peer_liveliness_event.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <span>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>

namespace plexus::io::detail {

// The posted observer fan-out discipline. Every edge is POSTED on the BORROWED executor over a
// SNAPSHOT of the observer list: a callback that (un)registers an observer mutates the live list
// without invalidating the in-flight iteration — a same-turn remove is honored, a same-turn add
// takes effect on the next posted edge. The engine owns m_observers/m_executor; these read them
// through it (they are friends).

// Each post is one operation of the engine, so a closing engine settles only after the last of them ran; a settled
// engine posts nothing more.
template<typename Engine, typename Fn>
void post_counted(Engine &e, Fn fn)
{
    using policy_type = typename Engine::policy_type;
    if(e.m_ops.settled())
        return;
    e.m_ops.begin();
    policy_type::post(e.m_executor,
                      [&e, fn = std::move(fn)]() mutable
                      {
                          e.m_ops.end();
                          fn(e);
                      });
}

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
    post_counted(e, [ev, edge](Engine &eng) { fan_out(eng, [&](observer &o) { (o.*edge)(ev.id, ev.node_name, ev.kind); }); });
}

template<typename Engine>
void post_rejected(Engine &e, const lifecycle_event &ev)
{
    post_counted(e, [ev](Engine &eng) { fan_out(eng, [&](observer &o) { o.on_peer_rejected(ev.id, ev.node_name, ev.reason); }); });
}

template<typename Engine>
void post_lifecycle_edge(Engine &e, const lifecycle_event &ev)
{
    switch(ev.edge)
    {
        case lifecycle_edge::connected:
            return detail::post_edge(e, ev, &observer::on_peer_connected);
        case lifecycle_edge::disconnected:
            return detail::post_edge(e, ev, &observer::on_peer_disconnected);
        case lifecycle_edge::reconnected:
            return detail::post_edge(e, ev, &observer::on_peer_reconnected);
        case lifecycle_edge::dead:
            return detail::post_edge(e, ev, &observer::on_peer_dead);
        case lifecycle_edge::ready:
            return detail::post_edge(e, ev, &observer::on_peer_ready);
        case lifecycle_edge::rejected:
            return detail::post_rejected(e, ev);
    }
}

// The indirection that keeps a per-packet drop site off the synchronous observer path.
template<typename Engine>
void post_drop(Engine &e, const drop_event &ev)
{
    post_counted(e, [ev](Engine &eng) { fan_out(eng, [&](observer &o) { o.on_drop(ev); }); });
}

// Never inline from the session's teardown/refusal frame.
template<typename Engine>
void post_security(Engine &e, const security_event &ev)
{
    post_counted(e, [ev](Engine &eng) { fan_out(eng, [&](observer &o) { o.on_security(ev); }); });
}

// The fused verdict edge: posted on the borrowed executor over the observer snapshot, never inline —
// the same drop-flood guard as every other edge, since a teardown frame can settle the verdict.
template<typename Engine>
void post_liveliness(Engine &e, const peer_liveliness_event &ev)
{
    post_counted(e, [ev](Engine &eng) { fan_out(eng, [&](observer &o) { o.on_peer_liveliness(ev); }); });
}

// Keeps the single-writer ring discipline (push only on the executor turn, never the io thread).
// The span into io-thread-owned bytes does NOT survive the post, so the frame is COPIED into an
// owned buffer carried BY MOVE into the turn and the span rebuilt over it. This copy-into-turn is
// the inherent every-packet cost of wire capture.
template<typename Engine>
void post_wire(Engine &e, recording::wire_direction dir, std::uint64_t seq, const node_id &peer, std::span<const std::byte> bytes)
{
    std::vector<std::byte> owned(bytes.begin(), bytes.end());
    post_counted(e,
                 [dir, seq, peer, owned = std::move(owned)](Engine &eng)
                 {
                     const recording::wire_record rec{dir, seq, peer, 0u, std::span<const std::byte>{owned}};
                     fan_out(eng, [&](observer &o) { o.on_wire(rec); });
                 });
}

}

#endif
