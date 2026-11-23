#ifndef HPP_GUARD_PLEXUS_DEMO_UNMEASURABLE_FIXTURE_H
#define HPP_GUARD_PLEXUS_DEMO_UNMEASURABLE_FIXTURE_H

#include <cstdint>

namespace plexus::demo {

// clang-format off
inline std::uint32_t brace_on_signature_line(std::uint32_t value) {
    return value + 1;
}
// clang-format on

}

#endif
