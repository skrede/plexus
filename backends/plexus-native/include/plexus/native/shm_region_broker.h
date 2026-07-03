#ifndef HPP_GUARD_PLEXUS_NATIVE_SHM_REGION_BROKER_H
#define HPP_GUARD_PLEXUS_NATIVE_SHM_REGION_BROKER_H

#if defined(_WIN32)
    #include "plexus/native/win_shm_region_broker.h"
#else
    #include "plexus/native/posix_shm_region_broker.h"
#endif

namespace plexus::native {

// The platform's compiled shared-memory region broker and its owning handle: the POSIX
// shm_open/mmap pair on Linux/macOS, the Win32 file-mapping pair on Windows. Both satisfy the
// core region_broker concept and expose the same create/attach/set_attach_policy surface, so a
// cross-platform consumer names these aliases instead of a concrete backend type.
#if defined(_WIN32)
using shm_region_broker = win_shm_region_broker;
#else
using shm_region_broker = posix_shm_region_broker;
#endif

using shm_region_handle = shm_region_broker::region_handle;

}

#endif
