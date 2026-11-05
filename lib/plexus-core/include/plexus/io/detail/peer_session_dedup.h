#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DEDUP_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DEDUP_H

#include "plexus/io/node_name.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/remembered_demand.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/warn_line.h"
#include "plexus/io/detail/handshake_resolve.h"
#include "plexus/io/detail/peer_session_filing.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <string>
#include <vector>

namespace plexus::io::detail {

inline peer_kind kept_kind(const node_id &self_id, const node_id &proven) noexcept
{
    return arbitrate_dedup(self_id, proven) == dedup_decision::keep_outbound ? peer_kind::dialed : peer_kind::accepted;
}

inline void warn_resolution(log::logger &logger, const node_id &proven, peer_kind kept, bool crossed)
{
    warn_line line;
    line.append(crossed ? "plexus: crossed dial with " : "plexus: duplicate session with ");
    line.append_id(proven);
    line.append(crossed ? ", kept the " : ", kept the newer ");
    line.append(kept == peer_kind::dialed ? "dialed session" : "accepted session");
    logger.warn(line.view());
}

// The demand is copied out first: re-remembering it may grow the container it is read from. A name only the loser
// carried is forgotten, so no later session under it replays demand the survivor already serves.
template<typename Build>
void fold_demand(Build &build, const std::string &from, const std::string &to)
{
    const std::vector<remembered_demand> demands = build.messages.remembered_topics(from);
    for(const remembered_demand &d : demands)
    {
        build.messages.remember_demand(to, d.fqn, d.qos, d.type_id, d.type_name);
        if(from != to)
            build.messages.forget_remembered_demand(from, d.fqn);
    }
}

// The loser is retired under its own label and leaves service under a fresh one before the survivor reports, so the
// survivor writes its entries after the loser's are gone, and the loser's delayed teardown cannot reach a shared name.
template<typename Registry, typename Build, typename Slot>
void supersede(Registry &reg, Build &build, Slot &incumbent, const std::string &survivor_name)
{
    typename Registry::session_type &loser = *incumbent.session;
    detail::fold_demand(build, loser.msg_peer().node_name, survivor_name);
    if(build.on_session_superseded)
        build.on_session_superseded(incumbent.record.peer_id, loser.msg_peer().node_name, survivor_name);
    detail::demote_loser(loser);
    slot_filing::leave_service(reg, incumbent);
}

// A crossed pair keeps the kind the id tie-break names at both ends. Of two sessions of one kind the newer wins: the
// older is what a restarted peer left behind, or a second dial that reached the same host.
template<typename Registry, typename Build, typename Slot>
bool newcomer_survives(Registry &reg, Build &build, Slot &slot, Slot &incumbent, const node_id &proven, peer_kind kind)
{
    const bool crossed   = session_kind(*incumbent.session) != kind;
    const peer_kind kept = crossed ? kept_kind(build.fsm_cfg.self_id, proven) : kind;
    warn_resolution(build.logger, proven, kept, crossed);
    if(kept != kind)
    {
        if(kind == peer_kind::dialed)
            slot_filing::leave_service(reg, slot);
        return false;
    }
    detail::supersede(reg, build, incumbent, kind == peer_kind::accepted ? node_name_of(proven) : slot.record.node_name);
    return true;
}

// The session that completes second for a proven identity resolves the pair. A dialed session is never relabeled: its
// key is what hang_up and the expected identity read.
template<typename Registry, typename Build, typename Slot>
bool admit_complete(Registry &reg, Build &build, Slot &slot, const node_id &proven, peer_kind kind)
{
    Slot *incumbent = slot_filing::incumbent(reg, slot, proven);
    if(incumbent != nullptr && !detail::newcomer_survives(reg, build, slot, *incumbent, proven, kind))
        return false;
    if(kind == peer_kind::accepted)
        slot_filing::take_identity(reg, slot, proven);
    return true;
}

template<typename Registry, typename Slot>
bool superseded(Registry &reg, Slot &slot)
{
    if(!slot.session)
        return false;
    const typename Registry::session_type &s = *slot.session;
    if(detail::is_demoted(s))
        return true;
    bool other = false;
    if(s.is_complete())
        reg.for_each_connected([&](const node_id &, const typename Registry::session_type &o) { other = other || (&o != &s && o.peer_identity() == s.peer_identity()); });
    return other;
}

// The remote's close of a crossed pair's loser is no failure, so its drop is closed quietly instead of redialed.
template<typename Registry, typename Slot>
bool close_if_superseded(Registry &reg, Slot &slot, peer_kind kind)
{
    if(!detail::superseded(reg, slot))
        return false;
    if(kind == peer_kind::dialed)
        slot.driver.deny_redial();
    detail::quiet_tear_down(*slot.session);
    return true;
}

template<typename Registry, typename Build, typename Slot>
void wire_admit_complete(Registry &reg, Build &build, Slot &slot)
{
    slot.session->on_admit_complete([&reg, &build, &slot](const node_id &proven, peer_kind kind) { return detail::admit_complete(reg, build, slot, proven, kind); });
}

}

#endif
