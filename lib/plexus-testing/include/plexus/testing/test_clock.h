#ifndef HPP_GUARD_PLEXUS_TESTING_TEST_CLOCK_H
#define HPP_GUARD_PLEXUS_TESTING_TEST_CLOCK_H

#include <chrono>

namespace plexus::testing {

// The one virtual clock the deterministic test substrate advances by hand. It
// carries nanosecond resolution and reports is_steady=false so that the inproc
// step-executor and virtual-clock timer (both written against this exact member
// set) fire deterministically with no wall clock. now() reads the static cursor;
// advance(d) steps it forward; reset() returns it to the epoch between cases.
struct test_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<test_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

}

#endif
