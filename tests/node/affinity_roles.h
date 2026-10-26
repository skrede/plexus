#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_ROLES_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_ROLES_H

#include <cstdint>
#include <string_view>

namespace plexus::testing {

enum class role_outcome : std::uint8_t
{
    unknown,
    ok,
    not_tripped
};

role_outcome run_handover_role(std::string_view role);

role_outcome run_freertos_role(std::string_view role);

}

#endif
