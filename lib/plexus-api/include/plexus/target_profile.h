#ifndef HPP_GUARD_PLEXUS_TARGET_PROFILE_H
#define HPP_GUARD_PLEXUS_TARGET_PROFILE_H

#include "plexus/policy.h"

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/liveness_storage.h"
#include "plexus/io/fixed_peer_storage.h"
#include "plexus/io/peer_report_emitter.h"
#include "plexus/io/liveliness_peer_storage.h"

#include "plexus/graph/fixed_topic_storage.h"
#include "plexus/graph/null_graph_change_log.h"
#include "plexus/graph/std_map_topic_storage.h"
#include "plexus/graph/vector_graph_change_log.h"

#include <cstddef>

namespace plexus {

// A capacity-carrying tag: it is never instantiated, only pattern-matched by profile_traits, so the
// capacity integers live here, on the decorator, and never touch the mechanism policy.
template<typename Policy, std::size_t Peers, std::size_t Topics,
         typename Liveliness = io::default_liveliness_storage>
struct bounded
{
};

// The transitive-relay decorator: like bounded<> it is never instantiated, only pattern-matched by
// profile_traits, so composing it onto a profile is the single spelling that turns the peer_report
// emitter on. A node never spelled relay<> keeps the structurally-absent null emitter.
template<typename Profile>
struct relay
{
};

template<std::size_t Leases, std::size_t Deadlines, std::size_t Arbiters>
struct fixed_liveliness
{
    using monitor = io::fixed_liveness_storage<Leases, Deadlines>;
    using arbiter = io::fixed_liveliness_peer_storage<Arbiters>;
};

}

namespace plexus::detail {

// A bare mechanism Policy is a profile whose storage defaults to the heap twins, so every existing
// node<policy, transports...> spelling resolves through this primary unchanged.
template<typename P>
struct profile_traits
{
    using policy              = P;
    using peer_storage        = io::std_map_peer_storage;
    using topic_storage       = graph::std_map_topic_storage;
    using liveliness_storage  = io::default_liveliness_storage;
    using graph_change_log    = graph::vector_graph_change_log;
    using peer_report_emitter = io::null_peer_report_emitter;
};

template<typename Policy, std::size_t Peers, std::size_t Topics, typename Liveliness>
struct profile_traits<bounded<Policy, Peers, Topics, Liveliness>>
{
    using policy              = Policy;
    using peer_storage        = io::fixed_peer_storage<Peers>;
    using topic_storage       = graph::fixed_topic_storage<Topics>;
    using liveliness_storage  = Liveliness;
    using graph_change_log    = graph::null_graph_change_log;
    using peer_report_emitter = io::null_peer_report_emitter;
};

// The relay decorator preserves every storage/log alias of the profile it wraps and overrides only
// the emitter to the real twin, so relay<bounded<...>> stays MCU-shaped yet emits, and a relayed
// profile satisfies target_profile through the wrapped policy.
template<typename P>
struct profile_traits<relay<P>> : profile_traits<P>
{
    using peer_report_emitter = io::peer_report_emitter;
};

template<typename T>
concept target_profile = plexus::Policy<typename profile_traits<T>::policy>;

}

#endif
