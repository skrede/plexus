#ifndef HPP_GUARD_PLEXUS_HINTS_ROBOTICS_H
#define HPP_GUARD_PLEXUS_HINTS_ROBOTICS_H

#include <cstdint>
#include <type_traits>

namespace plexus::hints::robotics {

// A schema_hint is an opaque 64-bit selector the core carries but never interprets. Its bit
// layout keeps concepts from distinct hint vocabularies from aliasing: the high 16 bits name a
// nonzero vocabulary id, the low 48 bits a concept ordinal within that vocabulary. The value 0
// is the universal no-hint sentinel and is never a concept, so a hint of 0 selects the opaque
// path regardless of vocabulary.
//
//   vocabulary id   owner
//   0x0000          reserved: universal no-hint sentinel
//   0x0001          robotics (this module)
//   0x0002 .. 0xffff  unallocated (a future plexus-hints-<domain> claims the next free id)
inline constexpr std::uint64_t vocabulary_id = 0x0001;

namespace detail {

inline constexpr std::uint64_t vocabulary_shift = 48;

constexpr std::uint64_t concept_value(std::uint64_t ordinal)
{
    return (vocabulary_id << vocabulary_shift) | ordinal;
}

}

// Domain-general robotics concepts. A concept names WHAT a payload is (a point cloud, a pose),
// leaving HOW it is decoded to whichever output backend translates the hint — this module holds
// no schema and names no vendor. A consumer sets a codec's schema_hint from to_hint(kind::…).
enum class kind : std::uint64_t
{
    point_cloud      = detail::concept_value(1),
    pose             = detail::concept_value(2),
    frame_transform  = detail::concept_value(3),
    compressed_image = detail::concept_value(4),
    raw_image        = detail::concept_value(5),
    laser_scan       = detail::concept_value(6),
    grid             = detail::concept_value(7),
    scene_update     = detail::concept_value(8),
};

constexpr std::uint64_t to_hint(kind k)
{
    return static_cast<std::underlying_type_t<kind>>(k);
}

}

#endif
