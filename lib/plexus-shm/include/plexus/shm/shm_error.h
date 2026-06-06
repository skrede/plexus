#ifndef HPP_GUARD_PLEXUS_SHM_SHM_ERROR_H
#define HPP_GUARD_PLEXUS_SHM_SHM_ERROR_H

#include <cstdint>

namespace plexus::shm {

// The backend-internal status the POSIX region calls map a raw errno onto. Leads
// with ok exactly as the core io_error / region_status families do, so the status
// reads identically at every call site. map_errno translates the irreducible
// errno set (the name exists, the perms deny, the region is too large, a foreign
// name) into one of these; the broker then folds shm_error onto the core
// region_status the concept returns, so core never sees an errno OR this enum.
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
