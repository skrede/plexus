#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_LIVELINESS_FUSE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_LIVELINESS_FUSE_H

#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include <cstdint>

namespace plexus::io::detail {

// The per-signal booleans a fuse rule reads. session_seen is "session is not absent"; hb_seen is
// "a heartbeat was recorded AND not invalidated by a later transport drop" (the staleness guard).
// A signal never seen for a peer does not vote, so a present-but-dead signal is distinguishable
// from an absent one.
struct signal_view
{
    bool session_up;
    bool session_seen;
    bool aware;
    bool hb_fresh;
    bool hb_seen;
};

enum class fuse_outcome : std::uint8_t
{
    alive,
    lost,
    no_verdict
};

// Any live signal keeps the peer alive; a peer with no entry never reaches this rule, so the else
// branch is only taken once at least one signal has been seen and all now read dead.
inline fuse_outcome fuse_any_signal_alive(const signal_view &sv)
{
    return (sv.session_up || sv.aware || sv.hb_fresh) ? fuse_outcome::alive : fuse_outcome::lost;
}

// The zero-added-state policy: the session edge is the whole truth; a never-connected peer yields
// no verdict rather than a false lost.
inline fuse_outcome fuse_session_authoritative(const signal_view &sv)
{
    if(sv.session_up)
        return fuse_outcome::alive;
    return sv.session_seen ? fuse_outcome::lost : fuse_outcome::no_verdict;
}

// Every PRESENT signal must assert alive; an absent signal does not vote (else a dialed-but-never-
// discovered peer could never read alive). Awareness is positive-only, so it never vetoes.
inline fuse_outcome fuse_all_required(const signal_view &sv)
{
    if(!sv.session_seen && !sv.hb_seen && !sv.aware)
        return fuse_outcome::no_verdict;
    const bool all_alive = (!sv.session_seen || sv.session_up) && (!sv.hb_seen || sv.hb_fresh);
    return all_alive ? fuse_outcome::alive : fuse_outcome::lost;
}

inline fuse_outcome fuse_signals(combine policy, const signal_view &sv)
{
    switch(policy)
    {
        case combine::any_signal_alive:
            return fuse_any_signal_alive(sv);
        case combine::session_authoritative:
            return fuse_session_authoritative(sv);
        case combine::all_required:
            return fuse_all_required(sv);
    }
    return fuse_outcome::no_verdict;
}

// The verdict's signal provenance: for alive, the signals asserting alive; for lost, the signals
// consulted and found dead (so a transport-drop loss is distinguishable from a silence loss).
inline liveliness_signal contributing_mask(const signal_view &sv, fuse_outcome outcome)
{
    liveliness_signal mask = liveliness_signal::none;
    if(outcome == fuse_outcome::alive)
    {
        if(sv.aware)
            mask = mask | liveliness_signal::awareness;
        if(sv.hb_fresh)
            mask = mask | liveliness_signal::heartbeat;
        if(sv.session_up)
            mask = mask | liveliness_signal::session;
        return mask;
    }
    if(sv.session_seen && !sv.session_up)
        mask = mask | liveliness_signal::session;
    if(sv.hb_seen && !sv.hb_fresh)
        mask = mask | liveliness_signal::heartbeat;
    return mask;
}

}

#endif
