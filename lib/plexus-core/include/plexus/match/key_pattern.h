#ifndef HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_H
#define HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_H

#include "plexus/match/key_pattern_error.h"
#include "plexus/match/key_pattern_bounds.h"

#include "plexus/match/detail/canonical.h"
#include "plexus/match/detail/segment_cursor.h"

#include "plexus/detail/expected.h"

#include <array>
#include <cstddef>
#include <string_view>

namespace plexus::match
{

// Validate-once value type ("parse, don't validate"): make() is the single
// validation path, returning expected<basic_key_pattern, key_pattern_error>. A
// constructed pattern holds only canonical, bounded bytes in an inline owning
// buffer and is unconditionally matchable — the relation kernels never revalidate.
template<typename Bounds = default_bounds>
class basic_key_pattern
{
public:
    static constexpr plexus::detail::expected<basic_key_pattern, key_pattern_error>
    make(std::string_view pattern) noexcept
    {
        std::array<detail::segment_desc, Bounds::k_pattern_depth> descriptors{};
        const auto count = detail::scan_canonical<Bounds>(pattern, descriptors);
        if(!count)
            return plexus::detail::unexpected{count.error()};
        return basic_key_pattern(pattern, descriptors, *count);
    }

    constexpr std::string_view text() const noexcept
    {
        return std::string_view(m_bytes.data(), m_length);
    }

    constexpr std::size_t segment_count() const noexcept
    {
        return m_count;
    }

    constexpr detail::segment_view segment(std::size_t index) const noexcept
    {
        const detail::segment_desc descriptor = m_segments[index];
        return detail::segment_view{
                std::string_view(m_bytes.data() + descriptor.offset, descriptor.length),
                descriptor.kind};
    }

private:
    std::size_t                                                m_count;
    std::size_t                                                m_length;
    std::array<char, Bounds::k_pattern_length>                 m_bytes;
    std::array<detail::segment_desc, Bounds::k_pattern_depth>  m_segments;

    constexpr basic_key_pattern(std::string_view canonical,
                                const std::array<detail::segment_desc, Bounds::k_pattern_depth> &descriptors,
                                std::size_t count) noexcept
            : m_count(count), m_length(canonical.size()), m_bytes{}, m_segments(descriptors)
    {
        for(std::size_t i = 0; i < canonical.size(); ++i)
            m_bytes[i] = canonical[i];
    }
};

using key_pattern = basic_key_pattern<default_bounds>;

}

#endif
