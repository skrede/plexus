// A second translation unit, so the target links the device facade header twice.
#include "plexus/freertos/device_runtime.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

TEST_CASE("device effective_park from a second translation unit keeps a park above the floor", "[seam]")
{
    const plexus::freertos::run_options opts{std::chrono::milliseconds{25}};
    REQUIRE(plexus::freertos::effective_park(opts) == std::chrono::milliseconds{25});
}
