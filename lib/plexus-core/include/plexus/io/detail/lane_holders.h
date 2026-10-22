#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_LANE_HOLDERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_LANE_HOLDERS_H

#include "plexus/io/upgrade_channel.h"

#include "plexus/detail/compat.h"

#include <string>
#include <vector>
#include <algorithm>
#include <string_view>

// Inside plexus::io::detail, plexus::detail must be fully qualified — io::detail shadows it.
namespace plexus::io::detail {

// A co-host peer the publisher offered its lane to; the lane carries its messages only once it answered ready.
struct lane_holder
{
    std::string node_name;
    reader_claim reader;
    bool ready;
    bool placed;
};

inline lane_holder *find_holder(std::vector<lane_holder> &holders, std::string_view node_name)
{
    const auto it = std::ranges::find(holders, node_name, &lane_holder::node_name);
    return it == holders.end() ? nullptr : &*it;
}

// Holders of one claim share its one reader, as two sessions of one peer do: the medium places that reader once, and
// each holder after the first switches to the lane on that placement.
inline bool place_claim(std::vector<lane_holder> &holders, plexus::detail::move_only_function<bool(reader_claim)> &place, reader_claim reader)
{
    const bool served = std::ranges::any_of(holders, [&reader](const lane_holder &h) { return h.placed && h.reader == reader; });
    if(reader == k_no_reader || (!served && !place(reader)))
        return false;
    for(lane_holder &h : holders)
        h.placed = h.placed || h.reader == reader;
    return true;
}

}

#endif
