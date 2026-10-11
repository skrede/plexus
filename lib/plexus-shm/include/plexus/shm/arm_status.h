#ifndef HPP_GUARD_PLEXUS_SHM_ARM_STATUS_H
#define HPP_GUARD_PLEXUS_SHM_ARM_STATUS_H

#include <cstdint>

namespace plexus::shm {

enum class arm_status : std::uint8_t
{
    armed,
    denied,
    unsupported,
    exhausted
};

}

#endif
