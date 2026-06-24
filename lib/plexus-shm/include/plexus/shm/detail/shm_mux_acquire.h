#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_MUX_ACQUIRE_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_MUX_ACQUIRE_H

#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_selection.h"
#include "plexus/shm/ring_geometry_mode.h"

#include "plexus/io/endpoint.h"

#include <memory>
#include <string>
#include <cstdint>

namespace plexus::shm::detail {

// The per-fqn provisioned geometry the shm member keys by topic. Producer-side, never
// wire-advertised (consumer_capacity 0 = the shipped floor).
struct shm_provisioned
{
    std::uint32_t max_payload       = 0;
    shm::ring_geometry_mode mode    = shm::ring_geometry_mode::reliable_preserving;
    std::uint32_t consumer_capacity = 0;
};

// The resolved acquire arguments for one (fqn, direction): the request direction (publisher)
// sizes from the provisioned width; the response direction (subscriber) passes 0 (the default
// ring).
struct shm_resolved_geometry
{
    std::uint32_t max_payload;
    shm::ring_geometry_mode mode;
    std::uint32_t consumer_capacity;
};

template<typename Member>
shm_resolved_geometry mux_resolve(const Member &m, const std::string &fqn, shm::ring_direction direction)
{
    auto it = m.m_geometry.find(fqn);
    if(it == m.m_geometry.end())
        return {shm::upgrade_ring_max_payload(direction, 0), shm::ring_geometry_mode::reliable_preserving, 0};
    const auto &p = it->second;
    return {shm::upgrade_ring_max_payload(direction, p.max_payload), p.mode, p.consumer_capacity};
}

// Mint a channel over the ring for ep, acquiring it first unless a prior can_acquire probe
// already holds it (channel_for non-null — reuse that held reference so the refcount stays at
// one held by the minted channel). nullptr on a broker failure. ep.address is the fqn.
template<typename Member>
auto mux_open(Member &m, const io::endpoint &ep, shm::acquire_mode amode = shm::acquire_mode::reclaim_stale) -> std::unique_ptr<typename Member::channel_type>
{
    auto *ch = m.m_registry.channel_for(ep.address, shm::ring_direction::request);
    if(ch == nullptr) // no probe held it: acquire fresh
    {
        const shm_resolved_geometry g = mux_resolve(m, ep.address, shm::ring_direction::request);
        if(m.m_registry.acquire(ep.address, shm::ring_direction::request, g.max_payload, g.mode, g.consumer_capacity, amode) == shm::acquire_result::failed)
            return nullptr;
        ch = m.m_registry.channel_for(ep.address, shm::ring_direction::request);
    }
    if(ch == nullptr)
        return nullptr;
    return std::make_unique<typename Member::channel_type>(m.m_registry, *ch, ep.address, ep);
}

}

#endif
