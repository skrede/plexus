#ifndef HPP_GUARD_PLEXUS_NATIVE_SHM_ERROR_H
#define HPP_GUARD_PLEXUS_NATIVE_SHM_ERROR_H

#include <cstdint>

namespace plexus::native {

// The backend-internal status map_errno folds a raw errno onto; the broker then
// folds it onto the core region_status, so core never sees an errno OR this enum.
enum class shm_error : std::uint8_t
{
    ok,
    name_invalid,
    already_exists,
    not_found,
    permission_denied,
    no_space,
    map_failed,
    unknown
};

}

#endif
