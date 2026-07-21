#ifndef HPP_GUARD_PLEXUS_IO_ROUTE_OPTIONS_H
#define HPP_GUARD_PLEXUS_IO_ROUTE_OPTIONS_H

#include <cstddef>
#include <cstdint>

namespace plexus::io
{

// How an arriving direct candidate is kept from being displaced by a transitive flood. Under
// reserved_slots a fixed portion of each identity's candidate array is off-limits to transitive
// entries, so a direct always has room without eviction; under priority_evict transitive entries
// may fill every free slot but yield one back when a direct arrives. Both hold the invariant that
// a transitive candidate never displaces a row holding a direct one.
enum class direct_protection : std::uint8_t
{
    reserved_slots,
    priority_evict
};

// The consumer QoS knob for candidate admission, threaded through the routing_engine ctor. It is
// not a capacity: the per-identity candidate bound stays a storage template parameter. reserved_direct
// is the count of direct-only rows under reserved_slots (clamped to the candidate bound), ignored
// under priority_evict.
struct route_options
{
    direct_protection protection{direct_protection::reserved_slots};
    std::size_t reserved_direct{1};
};

}

#endif
