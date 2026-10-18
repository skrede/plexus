#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SAME_HOST_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SAME_HOST_H

#include "plexus/detail/node_upgrade_wiring.h"

#include <tuple>
#include <string>
#include <cstddef>
#include <string_view>
#include <type_traits>

namespace plexus::detail {

// Keys on the member's name-free geometry surface (default_geometry + the opaque-pointer recover)
// rather than the concrete override type, so the probe names no shm type: a member that provisions
// per-topic rings matches; a plain leaf does not.
template<typename M>
constexpr bool member_has_shm_ring = requires(M &m) {
    m.set_topic_geometry(std::string{}, std::size_t{}, m.default_geometry());
    m.geometry_from(static_cast<const void *>(nullptr));
};

// True when the pack carries a member with the shm-ring surface — the fallback self-carrier (a
// node-local shm self-ring) is available when no intra-node transport is present to serve self.
template<typename... Transports>
constexpr bool pack_has_shm_member = (member_has_shm_ring<Transports> || ...);

// The if-constexpr capability check makes a composition with no shm member a compile-time no-op.
template<typename Node, typename F>
void for_each_shm_member(Node &n, F &&fn)
{
    if constexpr(std::tuple_size_v<typename Node::transport_tuple> == 1)
    {
        if constexpr(member_has_shm_ring<typename Node::engine_transport>)
            fn(n.m_leaf);
    }
    else
    {
        n.m_leaf.for_each_member(
                [&](auto &m)
                {
                    using M = std::remove_reference_t<decltype(m)>;
                    if constexpr(member_has_shm_ring<M>)
                        fn(m);
                });
    }
}

// An absent per-topic override (null) resolves to each shm member's own default_geometry(); a
// composition with no shm member is a no-op. The override is the opaque carrier the declare
// seam crosses; each member recovers its own geometry type from it member-side.
template<typename Node>
void provision_same_host_ring(Node &n, std::string_view fqn, std::size_t effective_bytes, const void *geometry)
{
    std::string key{fqn};
    for_each_shm_member(n, [&](auto &m) { m.set_topic_geometry(key, effective_bytes, m.geometry_from(geometry)); });
}

// No shm member is inert: no gate and no policy installed.
template<typename Node>
void install_upgrade_coordinator(Node &n)
{
    for_each_shm_member(n,
                        [&n](auto &m)
                        {
                            n.m_engine.coordinator().on_policy(m.upgrade_policy());
                            install_same_host_upgrade(n.m_engine, m);
                        });
}

}

#endif
