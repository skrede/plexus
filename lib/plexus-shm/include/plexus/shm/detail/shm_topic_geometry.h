#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_GEOMETRY_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_GEOMETRY_H

#include "plexus/shm/detail/shm_mux_acquire.h"

#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_selection.h"

#include "plexus/io/same_host_geometry.h"

#include "plexus/detail/compat.h"

#include <string>
#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace plexus::shm::detail {

using shm_geometry_map = std::unordered_map<std::string, shm_provisioned>;

// What the publish fan-out's per-message wire_fallback size check reads for a topic.
struct shm_route_geometry
{
    io::ring_geometry_mode mode = io::ring_geometry_mode::reliable_preserving;
    std::uint64_t slot_capacity = 0;
};

// A LOCAL value (a remote peer cannot inject a geometry); an fqn with no entry resolves to the default ring.
inline void topic_set_geometry(shm_geometry_map &geometry, const std::string &fqn, std::size_t effective_bytes, io::same_host_geometry geom)
{
    geometry.insert_or_assign(fqn, shm_provisioned{static_cast<std::uint32_t>(effective_bytes), geom.mode, geom.max_consumers});
}

// An unprovisioned fqn resolves to the reliable_preserving default.
inline shm_route_geometry topic_route_geometry(const shm_geometry_map &geometry, const std::string &fqn)
{
    auto it = geometry.find(fqn);
    if(it == geometry.end())
        return {};
    const auto &p                 = it->second;
    const shm::ring_geometry geom = shm::ring_geometry_for(p.max_payload, p.mode, p.consumer_capacity);
    return {p.mode, geom.slot_capacity};
}

// Captures the resolved geometry by value, so the route is fixed at mint time.
inline plexus::detail::move_only_function<bool(std::size_t)> topic_companion_route(const shm_geometry_map &geometry, const std::string &fqn)
{
    const shm_route_geometry g = topic_route_geometry(geometry, fqn);
    return [mode = g.mode, cap = g.slot_capacity](std::size_t bytes) { return shm::route_message_medium(mode, bytes, cap) == shm::same_host_medium::shm; };
}

}

#endif
