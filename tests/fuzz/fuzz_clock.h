#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_CLOCK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_CLOCK_H

#include <chrono>

// A virtual clock for the inproc timers, so a harness moves time as the input says. The clock concept reads now()
// statically, so the time is static state, and every input starts by resetting it.
struct fuzz_clock
{
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    using time_point = std::chrono::time_point<fuzz_clock>;

    static constexpr bool is_steady = true;
    static inline time_point current{};

    static time_point now() noexcept
    {
        return current;
    }

    static void reset() noexcept
    {
        current = time_point{};
    }

    static void advance(duration d) noexcept
    {
        current += d;
    }
};

#endif
