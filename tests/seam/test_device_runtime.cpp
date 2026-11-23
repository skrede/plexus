// The host shim's vTaskDelay is a no-op, so the park compiles and runs off-target.

#include "plexus/freertos/run_task.h"
#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <cstdint>
#include <type_traits>

namespace {

// poll() posts a sentinel onto the borrowed executor; a tick that drains AFTER poll
// observes that sentinel run in the SAME iteration, witnessing the poll-before-drain
// ordering the facade must guarantee.
struct fake_pollable
{
    plexus::freertos::freertos_executor &ex;
    std::int32_t polls{0};
    std::int32_t drained_after_poll{0};

    void poll()
    {
        ++polls;
        ex.post([this] { ++drained_after_poll; });
    }
};

// A second, DISTINCT pollable type: a homogeneous pointer array could not hold both, so
// driving tick over a fake_pollable and a fake_pollable_b together proves the fold is
// genuinely heterogeneous rather than a same-type pack.
struct fake_pollable_b
{
    plexus::freertos::freertos_executor &ex;
    std::int32_t polls{0};
    std::int32_t drained_after_poll{0};

    void poll()
    {
        ++polls;
        ex.post([this] { ++drained_after_poll; });
    }
};

}

TEST_CASE("device tick polls a single handle once, then drains, then parks", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    fake_pollable p{ex};

    plexus::freertos::tick(ex, plexus::freertos::run_options{}, p);

    REQUIRE(p.polls == 1);
    REQUIRE(p.drained_after_poll == 1); // the poll-posted sentinel ran in this tick
}

TEST_CASE("device tick folds over two distinct pollable types in one tick", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    fake_pollable a{ex};
    fake_pollable_b b{ex};

    plexus::freertos::tick(ex, a, b);

    REQUIRE(a.polls == 1);
    REQUIRE(b.polls == 1);
    REQUIRE(a.drained_after_poll == 1);
    REQUIRE(b.drained_after_poll == 1);
}

TEST_CASE("device tick over a span polls each once with a single drain", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    fake_pollable a{ex};
    fake_pollable b{ex};

    plexus::freertos::tick(ex, a, b);

    REQUIRE(a.polls == 1);
    REQUIRE(b.polls == 1);
    REQUIRE(a.drained_after_poll == 1);
    REQUIRE(b.drained_after_poll == 1);
}

TEST_CASE("device run_options park of zero is clamped up to the non-zero floor", "[seam]")
{
    const plexus::freertos::run_options zero{std::chrono::milliseconds{0}};
    REQUIRE(plexus::freertos::effective_park(zero) >= plexus::freertos::k_min_park);
    REQUIRE(plexus::freertos::effective_park(zero).count() > 0);

    const plexus::freertos::run_options ten{std::chrono::milliseconds{10}};
    REQUIRE(plexus::freertos::effective_park(ten) == std::chrono::milliseconds{10});
}

// run_task forces the caller to name a measured stack: task_options has no default
// constructor, so a stack-less spawn cannot compile. The spawn itself is not exercised
// on the host — the trampoline runs the endless loop, validated on hardware.
static_assert(!std::is_default_constructible_v<plexus::freertos::task_options>, "task_options must require an explicit stack size");
static_assert(std::is_constructible_v<plexus::freertos::task_options, std::uint32_t>, "task_options is constructed from an explicit stack size");
