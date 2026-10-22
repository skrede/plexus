#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SIDECAR_PROBE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SIDECAR_PROBE_H

#include "plexus/native/shm_error.h"

#include "plexus/native/detail/posix_shm_status.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <filesystem>

#include <unistd.h>

#include <sys/stat.h>

namespace plexus::testing {

// Where a sidecar lock file carries a region's slots; elsewhere the region's own record locks carry them.
#if defined(__APPLE__)
inline constexpr bool k_sidecar_host = true;

inline std::filesystem::path sidecar_file(const std::string &canonical)
{
    std::array<char, 1024> dir{};
    const std::size_t counted = ::confstr(_CS_DARWIN_USER_TEMP_DIR, dir.data(), dir.size());
    REQUIRE((counted > 1 && counted <= dir.size()));
    return std::filesystem::path(dir.data()) / (canonical.substr(1) + ".lk");
}
#else
inline constexpr bool k_sidecar_host = false;

inline std::filesystem::path sidecar_file(const std::string &)
{
    return {};
}
#endif

inline bool sidecar_present(const std::string &canonical)
{
    const std::filesystem::path file = sidecar_file(canonical);
    return !file.empty() && std::filesystem::exists(file);
}

// Zero where no sidecar lock file is present.
inline std::uint64_t sidecar_inode(const std::string &canonical)
{
    const std::filesystem::path file = sidecar_file(canonical);
    struct stat st{};
    return !file.empty() && ::stat(file.c_str(), &st) == 0 ? static_cast<std::uint64_t>(st.st_ino) : 0;
}

inline std::string canonical_of(const std::string &name)
{
    std::string canonical;
    REQUIRE(plexus::native::detail::sanitize_region_name(name, canonical) == plexus::native::shm_error::ok);
    return canonical;
}

}

#endif
