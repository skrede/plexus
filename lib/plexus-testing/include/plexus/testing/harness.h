#ifndef HPP_GUARD_PLEXUS_TESTING_HARNESS_H
#define HPP_GUARD_PLEXUS_TESTING_HARNESS_H

#include "plexus/testing/mock_policy.h"
#include "plexus/testing/test_clock.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include <memory>

namespace plexus::testing {

// The deterministic test fixture: owns the inproc bus and step-executor on the
// virtual clock and exposes the documented payoff — advance(d) steps the clock and
// drains to quiescence in one call, so a case crosses a timeout deterministically
// without a wall clock. drive() drains posted work and pending deliveries without
// moving time. The make_* factories hand back blocks already bound to this
// executor so the clock/executor wiring lives in one place.
struct harness
{
    plexus::inproc::inproc_bus<test_clock>      bus;
    plexus::inproc::inproc_executor<test_clock> ex{bus};

    harness() { test_clock::reset(); }

    harness(const harness &)            = delete;
    harness &operator=(const harness &) = delete;
    harness(harness &&)                 = delete;
    harness &operator=(harness &&)      = delete;

    // Step the virtual clock forward and drain every post, timer, and delivery the
    // step yields. This is the single edge a case uses to cross a timeout.
    void advance(test_clock::duration d)
    {
        test_clock::advance(d);
        ex.drain();
    }

    // Drain posted work and pending deliveries to quiescence without moving time.
    void drive() { ex.drain(); }

    [[nodiscard]] mock_channel<test_clock> make_channel() { return mock_channel<test_clock>{ex}; }

    [[nodiscard]] std::unique_ptr<mock_channel<test_clock>> make_channel_ptr()
    {
        return std::make_unique<mock_channel<test_clock>>(ex);
    }

    [[nodiscard]] plexus::inproc::inproc_timer<test_clock> make_timer()
    {
        return plexus::inproc::inproc_timer<test_clock>{ex};
    }
};

}

#endif
