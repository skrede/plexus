#ifndef HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_EXCEPTION_FIXTURE_H
#define HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_EXCEPTION_FIXTURE_H

namespace plexus::demo {

template<typename T>
T grown_on_screen(T value)
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
    value += 1;
    return value;
}

template<typename T>
T overloaded_on_screen(T value)
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

template<typename T>
T overloaded_on_screen(T value, T step)
{
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    value += step;
    return value;
}

}

#endif
