#ifndef HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_ERROR_H
#define HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_ERROR_H

#include <cstdint>

namespace plexus::match
{

enum class key_pattern_error : std::uint8_t
{
    empty,
    too_long,
    too_deep,
    malformed,
    non_canonical
};

}

#endif
