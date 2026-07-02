#include "plexus/shm/region_naming.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <string_view>

// The region name is a fixed 16 hex chars regardless of fqn/direction/namespace length; the
// macOS backend composes its kernel-object names off that fixed width and must stay within the
// Darwin PSHMNAMLEN (~31) budget. These are platform-neutral naming-math invariants provable on
// any host.

using namespace plexus::shm;

namespace {

constexpr std::string_view k_region_prefix   = "/plexus.";
constexpr std::string_view k_slab_suffix     = ".s";
constexpr std::string_view k_notifier_prefix = "/pxn.";
constexpr std::size_t k_darwin_name_max      = 31;

const std::array<std::string, 3> k_fqns{
    std::string{"a"},
    std::string{"robot/telemetry/imu"},
    std::string(200, 'x'),
};

}

TEST_CASE("region_naming: fixed-width names fit the macOS budget and stay distinct", "[shm][naming]")
{
    SECTION("the name is a fixed 16 hex chars for any fqn/direction/namespace")
    {
        for(const auto &fqn : k_fqns)
        {
            REQUIRE(region_name_for(fqn, ring_direction::request).size() == 16);
            REQUIRE(region_name_for(fqn, ring_direction::response).size() == 16);
            REQUIRE(region_name_for(fqn, ring_direction::request, "").size() == 16);
            REQUIRE(region_name_for(fqn, ring_direction::request, "a-long-region-namespace").size() == 16);
        }
    }

    SECTION("the macOS canonical, slab, and notifier names fit the 31-char budget")
    {
        for(const auto &fqn : k_fqns)
        {
            const std::string name = region_name_for(fqn, ring_direction::request);

            const std::size_t region_len   = k_region_prefix.size() + name.size();
            const std::size_t slab_len     = region_len + k_slab_suffix.size();
            const std::size_t notifier_len = k_notifier_prefix.size() + name.size();

            REQUIRE(region_len == 24);
            REQUIRE(slab_len == 26);
            REQUIRE(notifier_len == 21);
            REQUIRE(region_len <= k_darwin_name_max);
            REQUIRE(slab_len <= k_darwin_name_max);
            REQUIRE(notifier_len <= k_darwin_name_max);
        }
    }

    SECTION("distinct fqns, directions, and namespaces yield distinct names")
    {
        const std::string a = "robot/telemetry/imu";
        const std::string b = "robot/telemetry/gps";

        REQUIRE(region_name_for(a, ring_direction::request) != region_name_for(b, ring_direction::request));
        REQUIRE(region_name_for(a, ring_direction::request) != region_name_for(a, ring_direction::response));
        REQUIRE(region_name_for(a, ring_direction::request, "app-a") != region_name_for(a, ring_direction::request, "app-b"));
        REQUIRE(region_name_for(a, ring_direction::request, "app-a") != region_name_for(a, ring_direction::request));
    }
}
