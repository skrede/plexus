#ifndef HPP_GUARD_TESTS_INTEGRATION_SUBSCRIBE_RETRY_RELAY_H
#define HPP_GUARD_TESTS_INTEGRATION_SUBSCRIBE_RETRY_RELAY_H

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"

#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <algorithm>
#include <functional>
#include <string_view>

namespace subscribe_retry_fixture {

enum class relay_action : std::uint8_t
{
    pass,
    hold,
    drop,
};

using relay_rule = std::function<relay_action(plexus::wire::msg_type, std::uint32_t)>;

struct recording_logger final : plexus::log::logger
{
    void warn(std::string_view line) override
    {
        lines.emplace_back(line);
    }

    std::size_t count(std::string_view needle) const
    {
        return static_cast<std::size_t>(std::ranges::count_if(lines, [needle](const std::string &l) { return l.find(needle) != std::string::npos; }));
    }

    std::vector<std::string> lines;
};

// One direction of the link. The rule sees each frame's type and its 1-based ordinal among frames of that type.
struct relay_leg
{
    relay_rule rule;
    std::array<std::uint32_t, 256> seen{};
    std::vector<std::vector<std::byte>> held;

    std::uint32_t count(plexus::wire::msg_type type) const
    {
        return seen[static_cast<std::uint8_t>(type)];
    }

    std::uint32_t total() const
    {
        return std::accumulate(seen.begin(), seen.end(), std::uint32_t{0});
    }
};

// Applies the action to the listed ordinals of one type, or to every frame of it when none are listed.
inline relay_rule on(plexus::wire::msg_type type, relay_action action, std::vector<std::uint32_t> nths = {})
{
    return [type, action, nths](plexus::wire::msg_type t, std::uint32_t n)
    { return t == type && (nths.empty() || std::ranges::find(nths, n) != nths.end()) ? action : relay_action::pass; };
}

}

#endif
