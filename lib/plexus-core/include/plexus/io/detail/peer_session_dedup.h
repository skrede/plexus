#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DEDUP_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DEDUP_H

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

inline void warn_resolution(log::logger &logger, const node_id &proven, peer_kind kept)
{
    warn_line line;
    line.append("plexus: crossed dial with ");
    line.append_id(proven);
    line.append(kept == peer_kind::dialed ? ", kept the dialed session" : ", kept the accepted session");
    logger.warn(line.view());
}

// The demand is copied out first: re-remembering it may grow the container it is read from.
template<typename Build>
void fold_demand(Build &build, const std::string &from, const std::string &to)
{
    const std::vector<remembered_demand> demands = build.messages.remembered_topics(from);
    for(const remembered_demand &d : demands)
        build.messages.remember_demand(to, d.fqn, d.qos, d.type_id, d.type_name);
}

template<typename Registry, typename Build, typename Session>
void supersede(Registry &reg, Build &build, const node_id &id, Session &incumbent, const std::string &survivor_name)
{
    detail::fold_demand(build, incumbent.msg_peer().node_name, survivor_name);
    if(session_kind(incumbent) == peer_kind::dialed)
        reg.deny_redial(id);
    if(build.on_session_superseded)
        build.on_session_superseded(id, incumbent.msg_peer().node_name);
    detail::demote_loser(incumbent);
}

// An accepted session takes the identity its peer proved as its key and label, unless another session proves it too;
// such a pair is left to the crossed-pair resolution below.
template<typename Registry, typename Slot>
bool file_if_uncontested(Registry &reg, Slot &slot, const node_id &proven, peer_kind kind)
{
    if(kind != peer_kind::accepted || !slot_filing::uncontested(reg, slot, proven))
        return false;
    slot_filing::relabel(reg, slot, proven);
    slot_filing::file_under_identity(reg, slot, proven);
    return true;
}

// A crossed pair is two channels for one proven identity, so the session that completes second asks the registry
// for the other. Two sessions of the same kind are not a crossed pair and are left alone.
template<typename Registry, typename Build, typename Slot>
bool admit_complete(Registry &reg, Build &build, Slot &slot, const node_id &proven, peer_kind kind)
{
    if(detail::file_if_uncontested(reg, slot, proven, kind))
        return true;
    typename Registry::session_type *incumbent = nullptr;
    node_id incumbent_id{};
    reg.for_each_connected(
            [&](const node_id &id, typename Registry::session_type &s)
            {
                if(&s == &*slot.session || s.peer_identity() != proven || session_kind(s) == kind)
                    return;
                incumbent    = &s;
                incumbent_id = id;
            });
    if(incumbent == nullptr)
        return true;
    const peer_kind kept = kept_kind(build.fsm_cfg.self_id, proven);
    warn_resolution(build.logger, proven, kept);
    if(kept == kind)
        detail::supersede(reg, build, incumbent_id, *incumbent, slot.record.node_name);
    else if(kind == peer_kind::dialed)
        slot.driver.deny_redial();
    return kept == kind;
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
