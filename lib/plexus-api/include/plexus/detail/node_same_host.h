#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SAME_HOST_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SAME_HOST_H

#include "plexus/detail/node_upgrade_wiring.h"

#include "plexus/io/same_host_geometry.h"

#include <tuple>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>

namespace plexus::detail {

template<typename M>
constexpr bool member_has_shm_ring = requires(M &m) { m.set_topic_geometry(std::string{}, std::size_t{}, m.default_geometry()); };

template<typename... Transports>
constexpr std::size_t shm_member_count = (static_cast<std::size_t>(member_has_shm_ring<Transports>) + ... + 0);

// True when the pack carries a member with the shm-ring surface — the fallback self-carrier (a
// node-local shm self-ring) is available when no intra-node transport is present to serve self.
template<typename... Transports>
constexpr bool pack_has_shm_member = shm_member_count<Transports...> != 0;

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

// An absent per-topic override resolves to each shm member's own default_geometry(); a composition with no shm member
// is a no-op.
template<typename Node>
void provision_same_host_ring(Node &n, std::string_view fqn, std::size_t effective_bytes, std::optional<io::same_host_geometry> geometry)
{
    std::string key{fqn};
    for_each_shm_member(n, [&](auto &m) { m.set_topic_geometry(key, effective_bytes, geometry.value_or(m.default_geometry())); });
}

// Each shm member learns the node's id and universe before any ring is minted. No shm member is inert: no gate and no
// policy installed.
template<typename Node>
void install_upgrade_coordinator(Node &n, std::uint32_t universe)
{
    for_each_shm_member(n,
                        [&n, universe](auto &m)
                        {
                            m.bind_node(n.m_id, universe);
                            n.m_engine.coordinator().on_policy(m.upgrade_policy());
                            install_same_host_upgrade(n.m_engine, m);
                        });
}

// A member may outlive the node, so the node withdraws the counting its registry reports into before the engine goes.
template<typename Node>
void withdraw_fallback_counting(Node &n)
{
    for_each_shm_member(n, [](auto &m) { m.registry().on_fallback(nullptr); });
}

}

#endif
