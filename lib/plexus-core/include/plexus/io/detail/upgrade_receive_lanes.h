#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RECEIVE_LANES_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RECEIVE_LANES_H

#include "plexus/io/upgrade_channel.h"

#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace plexus::io::detail {

// A subscriber's lane toward one publisher's ring: the demand that publisher's offer may attach, holding the receive
// companion once one did.
struct receive_lane
{
    std::string fqn;
    upgrade_receive receive;
};

using receive_lanes = std::unordered_map<std::string, std::vector<receive_lane>>;

inline const receive_lane *find_receive_lane(const receive_lanes &held, std::string_view node_name, std::string_view fqn)
{
    auto it = held.find(std::string{node_name});
    if(it == held.end())
        return nullptr;
    const auto lane = std::ranges::find(it->second, fqn, &receive_lane::fqn);
    return lane == it->second.end() ? nullptr : &*lane;
}

inline receive_lane *find_receive_lane(receive_lanes &held, std::string_view node_name, std::string_view fqn)
{
    return const_cast<receive_lane *>(find_receive_lane(std::as_const(held), node_name, fqn));
}

inline bool any_receive_lane(const receive_lanes &held, std::string_view fqn)
{
    for(const auto &[name, lanes] : held)
        if(std::ranges::find(lanes, fqn, &receive_lane::fqn) != lanes.end())
            return true;
    return false;
}

// A marker for a lane that is not engaged is ignored.
inline void resume_receive_lane(receive_lanes &held, std::string_view node_name, std::string_view fqn, std::uint64_t first_sequence)
{
    if(receive_lane *lane = find_receive_lane(held, node_name, fqn); lane != nullptr && lane->receive.engaged())
        lane->receive.resume_cb(first_sequence);
}

// A publisher left with no lane leaves the table.
inline void erase_receive_lane(receive_lanes &held, std::string_view node_name, std::string_view fqn)
{
    auto it = held.find(std::string{node_name});
    if(it == held.end())
        return;
    std::erase_if(it->second, [&](const receive_lane &lane) { return lane.fqn == fqn; });
    if(it->second.empty())
        held.erase(it);
}

}

#endif
