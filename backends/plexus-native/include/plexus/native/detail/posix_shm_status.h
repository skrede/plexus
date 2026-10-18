#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SHM_STATUS_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SHM_STATUS_H

#include "plexus/native/shm_error.h"

#include "plexus/shm/region_broker_concept.h"

#include <string>
#include <string_view>

namespace plexus::native::detail {

// Validate a caller-supplied bare name and normalize it to the canonical POSIX
// object name the backend opens. Rejects empty names, names with an embedded
// slash, and names whose canonical form exceeds NAME_MAX -> name_invalid.
shm_error sanitize_region_name(std::string_view logical, std::string &out_canonical);

shm_error map_errno(int e);

// Fold the backend-internal errno mapping onto the core status the concept
// returns, so core never observes an errno or shm_error.
plexus::shm::region_status to_status(shm_error e);

}

#endif
