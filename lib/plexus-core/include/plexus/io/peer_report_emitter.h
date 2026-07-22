#ifndef HPP_GUARD_PLEXUS_IO_PEER_REPORT_EMITTER_H
#define HPP_GUARD_PLEXUS_IO_PEER_REPORT_EMITTER_H

#include "plexus/io/report_options.h"

#include "plexus/io/detail/peer_report_consumers.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/topic_hash.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/topic_declaration.h"

#include "plexus/node_id.h"

#include <map>
#include <set>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace plexus::io {

// The non-relay twin: a members-less struct whose note/withdraw/replay/reassert are no-ops, so a node
// not spelled relay<> pays zero for the transitive-relay emitter and instantiates no peer_report send
// path at all. It mirrors peer_report_emitter's surface exactly, so one template parameter threads
// both twins with no platform branch. (The null_graph_change_log precedent.)
struct null_peer_report_emitter
{
    template<typename Table, typename Sink>
    void note_origin(const report_universe_ctx &, const node_id &, std::uint32_t, const Table &, Sink &&) noexcept
    {
    }

    template<typename Sink>
    void withdraw(const node_id &, Sink &&) noexcept
    {
    }

    template<typename Sink>
    void replay(const node_id &, Sink &&) const noexcept
    {
    }

    template<typename Sink>
    void reassert(Sink &&) noexcept
    {
    }

    // Always reports no transition and never declines: a non-relay node emits nothing, so the
    // decline honor points the caller gates on this twin are inert with no relay-only state.
    bool mark_decline(const node_id &, bool) noexcept
    {
        return false;
    }

    void clear_decline(const node_id &) noexcept {}

    bool declines(const node_id &) const noexcept
    {
        return false;
    }

    std::size_t reported_count() const noexcept
    {
        return 0;
    }

    bool reports(const node_id &) const noexcept
    {
        return false;
    }
};

// The relay twin: lifts an attached origin's identity + topics-with-types from held session state,
// stamps the origin universe from host-side config (the handshake carries none), refuses to bridge an
// origin whose stamped universe the relay's own does not admit, and holds one current report per
// reported origin so a completed session replays them and a lost live path withdraws them.
class peer_report_emitter
{
public:
    // Lift + stamp + refuse-to-bridge. On admission the origin's current report is (re)recorded under a
    // freshly minted per-origin seq and broadcast; a refused origin never emits nor enters the replay set.
    template<typename Table, typename Sink>
    void note_origin(const report_universe_ctx &local, const node_id &origin, std::uint32_t origin_universe, const Table &table, Sink &&send)
    {
        if(declines(origin))
            return;
        auto report = build_assert(local, origin, origin_universe, table);
        if(!report)
            return;
        m_reported[origin] = *report;
        send(*report);
    }

    // The withdrawal twin: an origin still reported is dropped from the replay set and a
    // withdrawal-flagged report is sent; an origin never reported is a no-op.
    template<typename Sink>
    void withdraw(const node_id &origin, Sink &&send)
    {
        auto it = m_reported.find(origin);
        if(it == m_reported.end())
            return;
        const wire::peer_report retire = withdrawal_of(it->second);
        m_reported.erase(it);
        send(retire);
    }

    // Replay every held report to one newly-completed session, skipping the report about that
    // session's own peer so an origin is never re-announced to itself.
    template<typename Sink>
    void replay(const node_id &skip, Sink &&send) const
    {
        for(const auto &[origin, report] : m_reported)
            if(origin != skip && !declines(origin))
                send(report);
    }

    // Re-assert every held report with a fresh per-origin seq so a still-live-but-idle origin's
    // downstream row refreshes before awareness_ttl; driven on the relay's heartbeat cadence.
    template<typename Sink>
    void reassert(Sink &&send)
    {
        for(auto &[origin, report] : m_reported)
        {
            if(declines(origin))
                continue;
            report.seq = next_seq(origin);
            send(report);
        }
    }

    // Record the origin's current cooperative decline posture, returning true only on a transition so
    // the caller drives a withdrawal or a re-lift once per edge; the heartbeat carrier is level, so a
    // repeated same-state assert is a no-op and an un-decline restores lift/offer for free.
    bool mark_decline(const node_id &origin, bool declining)
    {
        if(declining)
            return m_declined.insert(origin).second;
        return m_declined.erase(origin) != 0;
    }

    // Drop an origin's decline state outright when its session tears down or it is forgotten — a
    // lifecycle prune that bounds the set, not a cooperative un-decline, so it reports no transition
    // and never re-lifts the origin. A returning same-identity peer re-levels the bit on its first
    // heartbeat.
    void clear_decline(const node_id &origin)
    {
        m_declined.erase(origin);
    }

    bool declines(const node_id &origin) const noexcept
    {
        return m_declined.find(origin) != m_declined.end();
    }

    std::size_t reported_count() const noexcept
    {
        return m_reported.size();
    }

    bool reports(const node_id &origin) const
    {
        return m_reported.find(origin) != m_reported.end();
    }

private:
    std::map<node_id, wire::peer_report> m_reported;
    // Retained across a withdraw so a re-note/re-assert keeps minting monotonically-rising seqs.
    std::map<node_id, std::uint16_t> m_seq;
    // Origins that asserted the cooperative decline bit: none is (re)announced or offered a relayed path.
    std::set<node_id> m_declined;

    template<typename Table>
    std::optional<wire::peer_report> build_assert(const report_universe_ctx &local, const node_id &origin, std::uint32_t origin_universe, const Table &table)
    {
        wire::peer_report pr;
        pr.origin          = origin;
        pr.origin_universe = origin_universe;
        pr.hop             = 1;
        // A pattern-universe relay carries its canonical local pattern (and the presence flag) so the
        // refuse-to-bridge intersect below — and the receiver's — match on the pattern instead of
        // refusing every origin against an empty peer pattern; a concrete-default node stays flagless.
        if(local.pattern)
        {
            pr.flags |= wire::k_peer_report_universe_pattern_flag;
            pr.origin_universe_pattern = local.universe_pattern;
        }
        if(!detail::report_universe_admits(local, pr))
            return std::nullopt;
        pr.flags |= wire::k_peer_report_consent_flag | wire::k_peer_report_topics_flag;
        pr.topics = lift_topics(table, origin, local.max_report_topics);
        pr.seq    = next_seq(origin);
        return pr;
    }

    wire::peer_report withdrawal_of(const wire::peer_report &held)
    {
        wire::peer_report retire;
        retire.origin          = held.origin;
        retire.origin_universe = held.origin_universe;
        retire.hop             = held.hop;
        retire.flags           = wire::k_peer_report_withdrawal_flag;
        retire.seq             = next_seq(held.origin);
        return retire;
    }

    // Clamp the lifted set to `cap` (never above the decoder's ceiling) so the encoder's u16 count cannot wrap.
    template<typename Table>
    static std::vector<wire::topic_declaration> lift_topics(const Table &table, const node_id &origin, std::size_t cap)
    {
        const std::size_t ceiling = std::min(cap, wire::detail::k_peer_report_max_topics);
        std::vector<wire::topic_declaration> topics;
        table.for_each(
                [&](const graph::topic_record &rec)
                {
                    if(rec.node != origin || rec.role != graph::topic_role::publisher || topics.size() >= ceiling)
                        return;
                    topics.push_back(one_topic(rec));
                });
        return topics;
    }

    static wire::topic_declaration one_topic(const graph::topic_record &rec)
    {
        wire::topic_declaration td;
        td.topic_hash = wire::fqn_topic_hash(rec.name);
        td.type_id    = 0;
        td.fqn        = std::string{rec.name};
        td.state      = rec.types.count == 0 ? wire::type_state::undeclared : wire::type_state::declared;
        td.type_name  = rec.types.count == 0 ? std::string{} : std::string{rec.types.names[0]};
        return td;
    }

    std::uint16_t next_seq(const node_id &origin)
    {
        return m_seq[origin]++;
    }
};

}

#endif
