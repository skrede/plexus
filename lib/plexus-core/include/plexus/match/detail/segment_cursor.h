#ifndef HPP_GUARD_PLEXUS_MATCH_DETAIL_SEGMENT_CURSOR_H
#define HPP_GUARD_PLEXUS_MATCH_DETAIL_SEGMENT_CURSOR_H

#include <cstdint>
#include <string_view>

namespace plexus::match::detail
{

enum class segment_kind : std::uint8_t
{
    literal,
    single_star, // "*" — exactly one segment
    double_star  // "**" — zero or more segments
};

struct segment_desc
{
    std::uint16_t offset;
    std::uint16_t length;
    segment_kind  kind;
};

struct segment_view
{
    std::string_view text;
    segment_kind     kind;
};

constexpr segment_kind classify_segment(std::string_view segment) noexcept
{
    if(segment == "*")
        return segment_kind::single_star;
    if(segment == "**")
        return segment_kind::double_star;
    return segment_kind::literal;
}

// Forward split of a string_view over '/', yielding every chunk (empty chunks
// included, so leading/trailing/double slashes surface to the validator). No
// container, no copy — each chunk is a sub-view of the source bytes.
class segment_cursor
{
public:
    constexpr explicit segment_cursor(std::string_view pattern) noexcept
            : m_rest(pattern), m_exhausted(false)
    {
    }

    constexpr bool has_next() const noexcept
    {
        return !m_exhausted;
    }

    constexpr std::string_view next() noexcept
    {
        const std::size_t slash = m_rest.find('/');
        if(slash == std::string_view::npos)
        {
            m_exhausted = true;
            return m_rest;
        }
        const std::string_view segment = m_rest.substr(0, slash);
        m_rest.remove_prefix(slash + 1);
        return segment;
    }

private:
    std::string_view m_rest;
    bool             m_exhausted;
};

}

#endif
