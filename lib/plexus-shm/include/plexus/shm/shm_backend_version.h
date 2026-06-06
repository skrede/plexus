#ifndef HPP_GUARD_PLEXUS_SHM_SHM_BACKEND_VERSION_H
#define HPP_GUARD_PLEXUS_SHM_SHM_BACKEND_VERSION_H

#include <string_view>

// Trivial non-empty translation unit for the gated shared-memory backend
// archive: a single compiled symbol so the STATIC library is never an empty
// archive (rejected by some toolchains) before the POSIX broker / futex bodies
// land. It also gives a link-AND-RUN handle to assert the gated target was
// actually linked, not merely compiled.

namespace plexus::shm {

std::string_view backend_version() noexcept;

}

#endif
