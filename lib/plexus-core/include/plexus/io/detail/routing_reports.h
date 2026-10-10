#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_REPORTS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_REPORTS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/route_candidate.h"

#include "plexus/io/detail/route_admission.h"

#include "plexus/graph/graph_change.h"
#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/wire/peer_report.h"
#include "plexus/wire/udp_dedup_window.h"
#include "plexus/wire/topic_declaration.h"

#include "plexus/node_id.h"

#include <vector>
#include <cstdint>
#include <optional>

namespace plexus::io::detail {

// Forward-declared: the session lifecycle fan defines it and itself calls into this header.
template<typename Engine>
bool any_session_for(Engine &e, const node_id &proven);

// A seq-fresh re-report for an origin currently marked UNREACHABLE-NOT-DEAD (its relay returned) flips
// the row back to reachable under the SAME identity — a reachable delta, never appeared (a re-appear
// would churn the identity at the notification level). The flip is gated on note_reported's
// seq-freshness in note_reported_candidate, so a stale report from the dead incarnation is rejected
// before it reaches here and cannot resurrect the row; mark_reachable_via reports a change only on a
// real transition, so a steady-state refresh of an already-reachable row is silent. The data-plane dedup
// window is dropped in step because the revived relay's splice restarts its sequence at 0 — otherwise
// the first re-forwarded frame drops as too_old against the stale window and delivery stays wedged.
template<typename Engine>
void recover_reachable_via(Engine &e, const node_id &origin, const node_id &via)
{
    if(!e.m_known_peers.mark_reachable_via(origin, via))
        return;
    e.m_messages.reset_forward_dedup(origin, via);
    e.bump_graph_generation(true, origin, graph::change_kind::reachable);
}

template<typename Engine>
bool has_row_via(const Engine &e, const node_id &origin, const node_id &via)
{
    for(const route_candidate &c : e.m_known_peers.candidates(origin))
        if(!c.is_direct() && c.reach.via == via)
            return true;
    return false;
}

// A reporter may install at most max_reported_origins DISTINCT origins: a report that would add a
// NEW (origin, via=reporter) row past the ceiling is rejected and counted, never evicting a held
// row and never touching a direct peer. A refresh of an origin the reporter already reaches passes.
template<typename Engine>
bool reporter_at_capacity(const Engine &e, const node_id &reporter, const node_id &origin)
{
    auto it = e.m_reporter_load.find(reporter);
    if(it == e.m_reporter_load.end() || it->second < e.m_report.max_reported_origins)
        return false;
    return !has_row_via(e, origin, reporter);
}

template<typename Engine>
void dec_reporter_load(Engine &e, const node_id &reporter)
{
    auto it = e.m_reporter_load.find(reporter);
    if(it != e.m_reporter_load.end() && --it->second == 0)
        e.m_reporter_load.erase(it);
}

// Release the per-reporter load of every transitive row a record holds, before the whole record
// is removed (a TTL sweep or a forget) rather than retired one via at a time.
template<typename Engine>
void release_all_reporter_load(Engine &e, const node_id &origin)
{
    for(const route_candidate &c : e.m_known_peers.candidates(origin))
        if(!c.is_direct() && c.reach.via)
            dec_reporter_load(e, *c.reach.via);
}

// The relay R is gone: its session died, so R can no longer withdraw the origins it reported. Rather
// than retire them — which would render a dead PATH byte-identical to a dead PEER (disappeared) — each
// origin reachable via R degrades to UNREACHABLE-NOT-DEAD in place: the identity, its via edge, and its
// topics are retained so the path recovers when R returns. Only this relay-death caller diverges;
// withdrawal (retire_reported_if_fresh) and aging (sweep_aged_awareness) keep retiring to disappeared
// through the untouched retire_reported funnel. Gather-then-mark so the update never mutates mid-scan.
template<typename Engine>
void mark_reports_via_unreachable(Engine &e, const node_id &via)
{
    std::vector<node_id> origins;
    e.m_known_peers.for_each_origin_via(via, [&](const node_id &o) { origins.push_back(o); });
    for(const node_id &o : origins)
        if(e.m_known_peers.mark_unreachable_via(o, via))
            e.bump_graph_generation(true, o, graph::change_kind::unreachable);
}

template<typename Engine>
route_candidate reported_candidate(const Engine &e, const node_id &reporter, const wire::peer_report &pr)
{
    route_candidate cand{};
    // A via-only row carries a default-constructed (empty-scheme) endpoint as reach.transport: it
    // is reachability hearsay, never a dial target. The engine resolves dials through the DIRECT
    // endpoint only (route_admission::direct_endpoint), so this row is structurally non-dialable.
    cand.reach      = graph::route{endpoint{}, reporter};
    cand.origin     = graph::provenance{graph::observation::reported, reporter};
    cand.hop        = pr.hop;
    cand.seq_window = wire::udp_dedup_window{e.m_report.dedup_depth};
    return cand;
}

// A reported origin has no session, so its topic edges must be folded here and retired on
// withdrawal/aging or they outlive the reachability that carried them. The origin's wire
// declarations fold under its own id as publisher edges.
template<typename Engine>
void fold_report_topics(Engine &e, const wire::peer_report &pr)
{
    for(const wire::topic_declaration &td : pr.topics)
    {
        const std::optional<std::uint64_t> tid = td.state == wire::type_state::undeclared ? std::nullopt : std::optional{td.type_id};
        const bool changed = e.m_topics.upsert(graph::topic_edge{pr.origin, td.fqn, td.type_name, tid, graph::topic_role::publisher}).changed;
        e.bump_graph_generation(changed, pr.origin, graph::change_kind::appeared);
    }
}

// Retires a reported origin's transitive row and, once no reachability remains, its topic edges —
// WITHOUT churning the identity (only reachability rows retire). A row still held by a direct
// session keeps both the id and its topics.
template<typename Engine>
void retire_reported(Engine &e, const node_id &origin, const node_id &via)
{
    if(!e.m_known_peers.remove_transitive(origin, via))
        return;
    dec_reporter_load(e, via);
    if(e.m_known_peers.contains(origin))
        return;
    e.m_reported.erase(origin);
    e.m_topics.remove_node(origin);
    e.bump_graph_generation(true, origin, graph::change_kind::disappeared);
}

// A withdrawal retires the origin's row via `via` only when its seq is fresh against that row's
// dedup window: a reordered or replayed stale withdrawal (a hostile relay alternating
// assert/withdraw, or a UDP-backed reorder) is dropped, so it cannot retire a re-asserted live row.
template<typename Engine>
void retire_reported_if_fresh(Engine &e, const node_id &origin, const node_id &via, std::uint16_t seq)
{
    if(!e.m_known_peers.withdraw_seq_fresh(origin, via, seq))
        return;
    retire_reported(e, origin, via);
}

// A reporter's session (re)completed: re-arm the dedup windows of the rows it feeds so its next
// report re-anchors on its current (possibly restarted-from-0) seq counter instead of deduping
// against a stale high-water mark from the prior incarnation.
template<typename Engine>
void reset_windows_for_reporter(Engine &e, const node_id &id)
{
    if(typename Engine::session_type *s = e.m_registry.session_for(id))
        e.m_known_peers.reset_reported_windows(s->peer_identity());
}

// The reported-candidate note path (hop budget + per-origin dedup, then write). A withdrawal-
// flagged report retires instead — a removal is never rate-limited. Only a first sighting bumps
// the graph appeared; a fresh re-report refreshes silently, a duplicate/over-budget report is a
// no-op. No dial and no direct-liveliness note: a reported identity is via-only.
template<typename Engine>
void note_reported_candidate(Engine &e, const node_id &reporter, const wire::peer_report &pr)
{
    if(pr.flags & wire::k_peer_report_withdrawal_flag)
        return retire_reported_if_fresh(e, pr.origin, reporter, pr.seq);
    if(pr.hop > e.m_report.hop_budget)
        return;
    if(pr.topics.size() > e.m_report.max_report_topics || reporter_at_capacity(e, reporter, pr.origin))
        return (void)++e.m_report_dropped;
    const auto oc = e.m_known_peers.note_reported(pr.origin, reported_candidate(e, reporter, pr), pr.seq, e.now_for_aging(), e.m_route_options);
    if(oc != detail::report_admit::noted_new && oc != detail::report_admit::refreshed)
        return;
    fold_report_topics(e, pr);
    if(oc == detail::report_admit::noted_new)
    {
        ++e.m_reporter_load[reporter];
        e.m_reported.insert(pr.origin);
        if(any_session_for(e, pr.origin))
            e.m_messages.suppress_relayed_from(pr.origin);
        e.bump_graph_generation(true, pr.origin, graph::change_kind::appeared);
    }
    else
        recover_reachable_via(e, pr.origin, reporter);
}

}

#endif
