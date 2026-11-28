#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_SHM_STATUS_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_SHM_STATUS_H

#include "plexus/native/shm_error.h"

#include "plexus/shm/region_broker_concept.h"

#include <windows.h>

#include <string>
#include <string_view>

namespace plexus::native::detail {

shm_error sanitize_region_name(std::string_view logical, std::string &out_canonical);

shm_error map_last_error(DWORD e);

plexus::shm::region_status to_status(shm_error e);

}

#endif
