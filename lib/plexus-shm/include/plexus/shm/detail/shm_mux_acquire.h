#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_MUX_ACQUIRE_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_MUX_ACQUIRE_H

#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/shm_selection.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/node_id.h"

#include <memory>
#include <string>
#include <cstdint>
#include <utility>

namespace plexus::shm::detail {

// The per-fqn provisioned geometry the shm member keys by topic. Producer-side, never
// wire-advertised (consumer_capacity 0 = the shipped floor).
struct shm_provisioned
{
    std::uint32_t max_payload       = 0;
    io::ring_geometry_mode mode     = io::ring_geometry_mode::reliable_preserving;
    std::uint32_t consumer_capacity = 0;
};

// The resolved acquire arguments for one (fqn, direction): the request direction (publisher)
// sizes from the provisioned width; the response direction (subscriber) passes 0 (the default
// ring).
struct shm_resolved_geometry
{
    std::uint32_t max_payload;
    io::ring_geometry_mode mode;
    std::uint32_t consumer_capacity;
};

template<typename Member>
shm_resolved_geometry mux_resolve(const Member &m, const std::string &fqn, shm::ring_direction direction)
{
    auto it = m.m_geometry.find(fqn);
    if(it == m.m_geometry.end())
        return {shm::upgrade_ring_max_payload(direction, 0), io::ring_geometry_mode::reliable_preserving, 0};
    const auto &p = it->second;
    return {shm::upgrade_ring_max_payload(direction, p.max_payload), p.mode, p.consumer_capacity};
}

// Mint a channel over the member's own ring for ep in role. A both-role open reuses the hold a prior can_acquire probe
// left (channel_for non-null), so the minted channel owns that one hold; any other role acquires its own. nullptr on a
// broker failure. ep.address is the fqn. lost runs if the ring's waiter is lost; with none the channel closes.
template<typename Member>
auto mux_open(Member &m, const io::endpoint &ep, shm::ring_role role, shm::waiter_loss::lost_fn lost = nullptr) -> std::unique_ptr<typename Member::channel_type>
{
    const node_id &self = m.m_self;
    auto *ch            = role == shm::ring_role::both ? m.m_registry.channel_for(ep.address, shm::ring_direction::request, self) : nullptr;
    if(ch == nullptr)
    {
        const shm_resolved_geometry g = mux_resolve(m, ep.address, shm::ring_direction::request);
        if(m.m_registry.acquire(ep.address, shm::ring_direction::request, self, role, g.max_payload, g.mode, g.consumer_capacity) == shm::acquire_result::failed)
            return nullptr;
        ch = m.m_registry.channel_for(ep.address, shm::ring_direction::request, self);
    }
    if(ch == nullptr)
        return nullptr;
    return std::make_unique<typename Member::channel_type>(m.m_registry, *ch, ep.address, self, ep, role, std::move(lost));
}

}

#endif
