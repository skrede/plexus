#ifndef HPP_GUARD_PLEXUS_MATCH_DETAIL_CANONICAL_H
#define HPP_GUARD_PLEXUS_MATCH_DETAIL_CANONICAL_H

#include "plexus/match/detail/segment_cursor.h"

#include "plexus/match/key_pattern_error.h"

#include "plexus/detail/expected.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::match::detail
{

constexpr bool has_stray_star(std::string_view segment) noexcept
{
    return segment.find('*') != std::string_view::npos && segment != "*" && segment != "**";
}

// The two canonical adjacency rules (zenoh keyexpr RFC): consecutive "**" folds
// to one "**", and "**/*" folds to "*/**"; both are refused so only the folded
// form is ever stored.
constexpr bool violates_canonical(segment_kind previous, segment_kind current) noexcept
{
    return previous == segment_kind::double_star &&
           (current == segment_kind::double_star || current == segment_kind::single_star);
}

constexpr segment_desc make_desc(std::string_view segment, std::string_view pattern, segment_kind kind) noexcept
{
    return segment_desc{
            static_cast<std::uint16_t>(segment.data() - pattern.data()),
            static_cast<std::uint16_t>(segment.size()),
            kind};
}

constexpr plexus::detail::expected<segment_kind, key_pattern_error>
check_segment(std::string_view segment, segment_kind previous) noexcept
{
    if(segment.empty() || has_stray_star(segment))
        return plexus::detail::unexpected{key_pattern_error::malformed};
    const segment_kind kind = classify_segment(segment);
    if(violates_canonical(previous, kind))
        return plexus::detail::unexpected{key_pattern_error::non_canonical};
    return kind;
}

// Single non-recursive forward pass: bounds first (refuse over-long/over-deep
// before touching content), then per-segment grammar + canonical adjacency.
// Fills the caller's fixed-capacity descriptor table and returns the count.
template<typename Bounds>
constexpr plexus::detail::expected<std::size_t, key_pattern_error>
scan_canonical(std::string_view pattern, std::array<segment_desc, Bounds::k_pattern_depth> &out) noexcept
{
    if(pattern.empty())
        return plexus::detail::unexpected{key_pattern_error::empty};
    if(pattern.size() > Bounds::k_pattern_length)
        return plexus::detail::unexpected{key_pattern_error::too_long};

    segment_cursor cursor(pattern);
    std::size_t    count    = 0;
    segment_kind   previous = segment_kind::literal;
    while(cursor.has_next())
    {
        if(count == Bounds::k_pattern_depth)
            return plexus::detail::unexpected{key_pattern_error::too_deep};
        const std::string_view segment = cursor.next();
        const auto             checked = check_segment(segment, previous);
        if(!checked)
            return plexus::detail::unexpected{checked.error()};
        out[count] = make_desc(segment, pattern, *checked);
        previous   = *checked;
        ++count;
    }
    return count;
}

}

#endif
