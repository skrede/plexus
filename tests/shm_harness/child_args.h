#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_CHILD_ARGS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_CHILD_ARGS_H

#include "topology_block.h"

#include <string>
#include <vector>
#include <cstdint>
#include <charconv>
#include <optional>
#include <string_view>
#include <system_error>

namespace shm_harness {

inline constexpr std::uint32_t k_node_fixed_args = 9;

struct node_args
{
    std::string block;
    std::uint32_t index;
    std::uint32_t count;
    std::string fqn;
    std::string role;
    bool hint;
    bool deny;
    bool crossed;
    std::string composition;
    std::vector<std::uint16_t> ports;
};

struct registry_args
{
    std::string block;
    std::uint32_t index;
    std::string fqn;
    std::string role;
};

inline std::optional<std::uint32_t> to_u32(std::string_view text)
{
    std::uint32_t value     = 0;
    const char *const end   = text.data() + text.size();
    const auto [last, errc] = std::from_chars(text.data(), end, value);
    if(errc != std::errc{} || last != end)
        return std::nullopt;
    return value;
}

inline std::optional<node_args> parse_node_args(const std::vector<std::string> &argv)
{
    const bool sized                         = argv.size() >= k_node_fixed_args;
    const std::optional<std::uint32_t> index = sized ? to_u32(argv[1]) : std::nullopt;
    const std::optional<std::uint32_t> count = sized ? to_u32(argv[2]) : std::nullopt;
    if(!index || !count || *count > k_max_nodes || *index >= *count || argv.size() != k_node_fixed_args + *count)
        return std::nullopt;

    node_args out{argv[0], *index, *count, argv[3], argv[4], argv[5] == "1", argv[6] == "1", argv[7] == "1", argv[8], {}};
    for(std::uint32_t i = 0; i < *count; ++i)
    {
        const std::optional<std::uint32_t> port = to_u32(argv[k_node_fixed_args + i]);
        if(!port || *port > 0xFFFFu)
            return std::nullopt;
        out.ports.push_back(static_cast<std::uint16_t>(*port));
    }
    return out;
}

inline std::optional<registry_args> parse_registry_args(const std::vector<std::string> &argv)
{
    const std::optional<std::uint32_t> index = argv.size() == 4 ? to_u32(argv[1]) : std::nullopt;
    if(!index || *index >= k_max_nodes)
        return std::nullopt;
    return registry_args{argv[0], *index, argv[2], argv[3]};
}

}

#endif
