#ifndef HPP_GUARD_PLEXUS_TESTING_PLATFORM_H
#define HPP_GUARD_PLEXUS_TESTING_PLATFORM_H

#include "plexus/detail/socket_compat.h"

#include <atomic>
#include <string>
#include <cstdint>
#include <filesystem>

#if defined(_WIN32)
    #include <windows.h>
#else
    #include <unistd.h>
#endif

namespace plexus::testing {

[[nodiscard]] inline std::uint32_t process_id()
{
#if defined(_WIN32)
    return static_cast<std::uint32_t>(::GetCurrentProcessId());
#else
    return static_cast<std::uint32_t>(::getpid());
#endif
}

[[nodiscard]] inline std::filesystem::path temp_directory()
{
    return std::filesystem::temp_directory_path();
}

// The pid + monotonic-counter token makes the name unique across concurrent
// processes and within a single process, the portable analog of mkdtemp's XXXXXX.
[[nodiscard]] inline std::filesystem::path make_temp_dir(const std::string &prefix)
{
    static std::atomic<std::uint64_t> counter{0};
    const std::uint64_t token   = counter.fetch_add(1, std::memory_order_relaxed);
    std::filesystem::path dir    = temp_directory() / (prefix + std::to_string(process_id()) + "-" + std::to_string(token));
    std::filesystem::create_directory(dir);
    return dir;
}

inline void remove_socket_path(const std::string &path)
{
    plexus::detail::remove_socket_path(path);
}

}

#endif
