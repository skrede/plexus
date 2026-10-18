#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_REGION_NAMES_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_REGION_NAMES_H

#include "plexus/shm/region_naming.h"

#include "plexus/discovery/universe.h"

#include "plexus/detail/node_identity.h"

#include "plexus/node_id.h"

#include <string>
#include <cstdint>
#include <string_view>

namespace plexus::testing {

// The publisher a test's registry-level and raw-ring roles agree on when no node identity is in play.
inline node_id test_publisher()
{
    return node_id_from_name("shm.test.publisher");
}

// The request ring a publisher owns for fqn.
inline std::string region_name(std::string_view fqn, const node_id &publisher, std::uint32_t universe = discovery::k_default_universe, std::string_view region_ns = {})
{
    return shm::region_name_for(fqn, publisher, universe, shm::ring_direction::request, region_ns);
}

inline std::string region_name(std::string_view fqn, std::string_view publisher_name, std::uint32_t universe = discovery::k_default_universe, std::string_view region_ns = {})
{
    return region_name(fqn, node_id_from_name(publisher_name), universe, region_ns);
}

// A bare shm member names its send rings with the id a node would bind; a test binds it to name the publisher.
template<typename Member>
void bind_test_publisher(Member &member, std::uint32_t universe = discovery::k_default_universe)
{
    member.bind_node(test_publisher(), universe);
}

}

#endif
