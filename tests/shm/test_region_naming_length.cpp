#include "plexus/shm/region_naming.h"
#include "plexus/shm/doorbell_naming.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

// The region name is a fixed 16 hex chars whatever the fqn, publisher, universe, direction and namespace; the macOS
// backend composes its kernel-object names off that fixed width, so its shared-memory names stay within the Darwin
// PSHMNAMLEN (31) budget and its semaphore doorbells within PSEMNAMLEN.

using namespace plexus::shm;

namespace {

constexpr std::string_view k_region_prefix = "/plexus.";
constexpr std::string_view k_slab_suffix   = ".s";
constexpr std::size_t k_darwin_name_max    = 31;
constexpr std::uint32_t k_universe_a       = 0x58E1B347u;
constexpr std::uint32_t k_universe_b       = 0x00000001u;

const std::array<std::string, 4> k_fqns{
        std::string{},
        std::string{"a"},
        std::string{"robot/telemetry/imu"},
        std::string(200, 'x'),
};

plexus::node_id publisher(std::uint8_t tag)
{
    plexus::node_id id{};
    id[15] = std::byte{tag};
    return id;
}

std::string name_of(std::string_view fqn, std::uint8_t pub = 1, std::uint32_t universe = k_universe_a, std::string_view ns = {}, ring_direction d = ring_direction::request)
{
    return region_name_for(fqn, publisher(pub), universe, d, ns);
}

}

TEST_CASE("region_naming: fixed-width names fit the macOS budget and stay distinct", "[shm][naming]")
{
    SECTION("the name is a fixed 16 hex chars for any fqn, publisher, universe, direction and namespace")
    {
        for(const auto &fqn : k_fqns)
        {
            REQUIRE(name_of(fqn).size() == 16);
            REQUIRE(name_of(fqn, 0xff, k_universe_b).size() == 16);
            REQUIRE(name_of(fqn, 1, k_universe_a, {}, ring_direction::response).size() == 16);
            REQUIRE(name_of(fqn, 1, k_universe_a, "a-long-region-namespace").size() == 16);
        }
    }

    SECTION("the macOS canonical and slab names fit the 31-char budget and the doorbell names the semaphore budget")
    {
        for(const auto &fqn : k_fqns)
        {
            const std::string name = name_of(fqn, 0xff, k_universe_b, "a-long-region-namespace");

            const std::size_t region_len   = k_region_prefix.size() + name.size();
            const std::size_t slab_len     = region_len + k_slab_suffix.size();
            const std::size_t doorbell_len = format_doorbell_name(k_semaphore_doorbell_prefix, name, 15, 0xFFFF'FFFFu).size;

            REQUIRE(region_len == 24);
            REQUIRE(slab_len == 26);
            REQUIRE(region_len <= k_darwin_name_max);
            REQUIRE(slab_len <= k_darwin_name_max);
            REQUIRE(doorbell_len <= k_doorbell_name_max);
            REQUIRE(doorbell_len <= 30);
        }
    }

    SECTION("distinct fqns, directions and namespaces yield distinct names")
    {
        REQUIRE(name_of("robot/telemetry/imu") != name_of("robot/telemetry/gps"));
        REQUIRE(name_of("robot/telemetry/imu") != name_of("robot/telemetry/imu", 1, k_universe_a, {}, ring_direction::response));
        REQUIRE(name_of("robot/telemetry/imu", 1, k_universe_a, "app-a") != name_of("robot/telemetry/imu", 1, k_universe_a, "app-b"));
        REQUIRE(name_of("robot/telemetry/imu", 1, k_universe_a, "app-a") != name_of("robot/telemetry/imu"));
    }

    SECTION("distinct publishers and distinct universes yield distinct names; equal tuples one name")
    {
        REQUIRE(name_of("robot/telemetry/imu", 1) != name_of("robot/telemetry/imu", 2));
        REQUIRE(name_of("robot/telemetry/imu", 1, k_universe_a) != name_of("robot/telemetry/imu", 1, k_universe_b));
        REQUIRE(name_of("robot/telemetry/imu", 7, k_universe_b, "app-a") == name_of(std::string{"robot/telemetry/imu"}, 7, k_universe_b, std::string{"app-a"}));
    }

    SECTION("adjacent fields never alias: the boundary between fqn and namespace is part of the key")
    {
        REQUIRE(name_of("ab", 1, k_universe_a, "c") != name_of("a", 1, k_universe_a, "bc"));
        REQUIRE(name_of("a", 1, k_universe_a, {}) != name_of({}, 1, k_universe_a, "a"));
    }

    SECTION("an empty fqn or namespace is a distinct name, and identity is byte equality with no normalization")
    {
        REQUIRE(name_of({}) != name_of("a"));
        REQUIRE(name_of("a", 1, k_universe_a, {}) != name_of("a", 1, k_universe_a, "n"));
        REQUIRE(name_of("robot/imu") != name_of("Robot/imu"));
        REQUIRE(name_of("robot/imu") != name_of("robot/imu/"));
    }
}
