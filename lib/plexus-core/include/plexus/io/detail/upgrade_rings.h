#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RINGS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RINGS_H

#include "plexus/detail/compat.h"

#include <string>
#include <vector>
#include <cstddef>
#include <memory>
#include <algorithm>
#include <string_view>
#include <unordered_map>

// These live in plexus::io::detail. The upgrade coordinator reaches them by namespace
// fall-through. Inside this namespace plexus::detail must be fully qualified — io::detail
// shadows it.
namespace plexus::io::detail {

// One held companion lane for a (peer, fqn). Exactly one lane is engaged per pair — the up-edge
// role decides — and both lanes are keyed identically, so the lookups treat them uniformly. The
// per-message route is an injected predicate the minting medium constructs: fits(bytes) is true
// when this message rides the companion channel, false when it falls back to the wire. An empty
// fits (the subscriber lane, or a publisher medium with no size gate) routes nothing on its own.
template<typename Channel, typename Receive>
struct coordinator_ring
{
    std::string                                           fqn;
    std::unique_ptr<Channel>                              channel; // publisher lane
    plexus::detail::move_only_function<bool(std::size_t)> fits;    // per-message route predicate
    Receive                                               receive; // subscriber lane
};

// The per-peer held lanes, keyed by node_name.
template<typename Channel, typename Receive>
using coordinator_held = std::unordered_map<std::string, std::vector<coordinator_ring<Channel, Receive>>>;

template<typename Channel, typename Receive>
const coordinator_ring<Channel, Receive> *find_ring(const coordinator_held<Channel, Receive> &held, std::string_view node_name, std::string_view fqn)
{
    auto it = held.find(std::string{node_name});
    if(it == held.end())
        return nullptr;
    for(const auto &r : it->second)
        if(r.fqn == fqn)
            return &r;
    return nullptr;
}

template<typename Channel, typename Receive>
bool any_holder(const coordinator_held<Channel, Receive> &held, std::string_view fqn)
{
    for(const auto &[name, rings] : held)
        for(const auto &r : rings)
            if(r.fqn == fqn)
                return true;
    return false;
}

// Drop the (peer, fqn) lane; returns true if the peer's whole list emptied (so the caller erases
// the peer key).
template<typename Channel, typename Receive>
bool erase_ring(coordinator_held<Channel, Receive> &held, std::string_view node_name, std::string_view fqn)
{
    auto it = held.find(std::string{node_name});
    if(it == held.end())
        return false;
    std::erase_if(it->second, [&](const auto &r) { return r.fqn == fqn; });
    if(it->second.empty())
    {
        held.erase(it);
        return true;
    }
    return false;
}

// The publish fan's per-message route: the held companion channel when the message fits the
// medium's injected predicate, else nullptr so the pair keeps the wire sub.channel (the
// dual-delivery fail-safe). An unheld pair, an empty predicate, or an over-cap message resolves
// to nullptr. The held map is taken mutably: the route invokes the medium's fits predicate (a
// move_only_function with a non-const call operator).
template<typename Channel, typename Receive>
Channel *route_companion(coordinator_held<Channel, Receive> &held, std::string_view node_name, std::string_view fqn, std::size_t bytes)
{
    auto it = held.find(std::string{node_name});
    if(it == held.end())
        return nullptr;
    for(auto &r : it->second)
        if(r.fqn == fqn)
            return (r.fits && r.fits(bytes)) ? r.channel.get() : nullptr;
    return nullptr;
}

}

#endif
