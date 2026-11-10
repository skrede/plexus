#ifndef HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_FOOTPRINT_H
#define HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_FOOTPRINT_H

#include <limits>
#include <string>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <filesystem>
#include <system_error>

namespace scale {

// The whole-process footprint as Linux reports it under /proc/self; a host without it reads 0 in every field.
struct footprint
{
    std::uint64_t rss_kib{};
    std::uint64_t open_fds{};
    std::uint64_t threads{};
};

inline std::uint64_t read_vmrss_kib()
{
    std::ifstream status{"/proc/self/status"};
    std::string key;
    while(status >> key)
    {
        if(key == "VmRSS:")
        {
            std::uint64_t value = 0;
            status >> value;
            return value;
        }
        status.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 0;
}

inline std::uint64_t count_dir_entries(const std::filesystem::path &path)
{
    std::error_code ec;
    std::filesystem::directory_iterator it{path, ec};
    if(ec)
        return 0;
    return static_cast<std::uint64_t>(std::distance(begin(it), end(it)));
}

inline footprint sample_footprint()
{
    return {read_vmrss_kib(), count_dir_entries("/proc/self/fd"), count_dir_entries("/proc/self/task")};
}

}

#endif
