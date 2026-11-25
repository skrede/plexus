#ifndef HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_SCREEN_FIXTURE_H
#define HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_SCREEN_FIXTURE_H

#include <cstdint>

namespace plexus::demo {

template<typename T>
T over_on_screen(T value)
{
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    return value;
}

inline std::uint32_t within_on_screen(std::uint32_t value)
{
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    return value;
}

}

#endif
