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
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The non-relay twin: a members-less struct whose note/withdraw/replay are no-ops, so a node not
// spelled relay<> pays zero for the transitive-relay emitter by construction and instantiates no
// peer_report send path at all. It mirrors peer_report_emitter's surface exactly, so one template
// parameter threads both twins with no platform branch. (The null_graph_change_log precedent.)
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
// stamps the origin universe from host-side config (the handshake carries none), refuses to bridge
// an origin whose stamped universe the relay's own does not admit, and holds one current report per
// reported origin so a newly-completed session replays them and a lost live path withdraws them.
class peer_report_emitter
{
public:
    // Lift + stamp + refuse-to-bridge. On admission the origin's current report is (re)recorded under
    // a freshly minted per-origin seq and handed to the sink for broadcast; a refused origin never
    // emits and never enters the replay set.
    template<typename Table, typename Sink>
    void note_origin(const report_universe_ctx &local, const node_id &origin, std::uint32_t origin_universe, const Table &table, Sink &&send)
    {
        auto report = build_assert(local, origin, origin_universe, table);
        if(!report)
            return;
        m_reported[origin] = *report;
        send(*report);
    }

    // The withdrawal twin: an origin still reported is dropped from the replay set and a
    // withdrawal-flagged report is handed to the sink; an origin never reported is a no-op, so a dead
    // origin is never re-refreshed downstream.
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
            if(origin != skip)
                send(report);
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
    std::map<node_id, std::uint16_t> m_seq;

    template<typename Table>
    std::optional<wire::peer_report> build_assert(const report_universe_ctx &local, const node_id &origin, std::uint32_t origin_universe, const Table &table)
    {
        wire::peer_report pr;
        pr.origin          = origin;
        pr.origin_universe = origin_universe;
        pr.hop             = 1;
        if(!detail::report_universe_admits(local, pr))
            return std::nullopt;
        pr.flags  = wire::k_peer_report_consent_flag | wire::k_peer_report_topics_flag;
        pr.topics = lift_topics(table, origin);
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

    template<typename Table>
    static std::vector<wire::topic_declaration> lift_topics(const Table &table, const node_id &origin)
    {
        std::vector<wire::topic_declaration> topics;
        table.for_each(
                [&](const graph::topic_record &rec)
                {
                    if(rec.node != origin || rec.role != graph::topic_role::publisher)
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
