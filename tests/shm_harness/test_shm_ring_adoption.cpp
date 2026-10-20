#include "launch_helpers.h"

#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/xproc_process.h"

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <numeric>
#include <optional>

// A ring outlives its creator's process on every platform: a restarted creator with the same publisher identity adopts
// it, after a clean exit or after its predecessor was killed with a claim in flight, and a second live creator is
// refused.

namespace {

using namespace std::chrono_literals;
using plexus::shm::ring_role;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::xproc_process;
using shm_harness::wait_until;

std::string adoption_fqn(const char *tag)
{
    return std::string("ring_adoption.") + tag + "." + std::to_string(plexus::testing::process_id());
}

std::optional<xproc_process> spawn_creator(const std::string &fqn, const char *mode, std::uint32_t first, std::uint32_t count)
{
    return xproc_process::spawn("shm_harness.ring_creator", {fqn, mode, std::to_string(first), std::to_string(count)});
}

// The reader's wake is this process's bus, so it is drained here until it has read count messages.
bool read_until(wake_bus &bus, const ring_holder &reader, std::size_t count)
{
    return wait_until(
            [&]
            {
                bus.ring();
                return reader.received().size() >= count;
            },
            10s);
}

std::vector<std::uint32_t> one_to(std::uint32_t last)
{
    std::vector<std::uint32_t> values(last);
    std::iota(values.begin(), values.end(), 1u);
    return values;
}

}

TEST_CASE("ring_adoption a restarted creator adopts the ring and the reader keeps receiving in publish order", "[shm][ring_adoption]")
{
    const bool killed = GENERATE(false, true);
    wake_bus bus;
    ring_view view{adoption_fqn(killed ? "killed" : "clean")};
    ring_holder reader{bus, view.fqn};
    std::optional<xproc_process> first = spawn_creator(view.fqn, killed ? "claim" : "clean", 1, 10);
    REQUIRE(first);
    bool joined = false;
    REQUIRE(wait_until([&] { return joined || (joined = reader.acquire(ring_role::consumer) != acquire_result::failed); }, 10s));
    REQUIRE(read_until(bus, reader, 10));
    if(killed)
    {
        REQUIRE(wait_until([&] { return view.map() && view.ring.tail_position() == 11; }, 10s));
        first->kill();
    }
    else
        REQUIRE(first->wait_for(10s) == 0);

    std::optional<xproc_process> second = spawn_creator(view.fqn, "clean", 11, 10);
    REQUIRE(second);
    REQUIRE(read_until(bus, reader, 20));
    REQUIRE(second->wait_for(10s) == 0);
    CHECK(reader.received() == one_to(20));
    reader.release(ring_role::consumer);
    view.control = plexus::testing::native_broker::region_handle{};
    view.slab    = plexus::testing::native_broker::region_handle{};
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_adoption a second creator is refused while the first lives", "[shm][ring_adoption]")
{
    ring_view view{adoption_fqn("refused")};
    std::optional<xproc_process> first = spawn_creator(view.fqn, "hold", 0, 0);
    REQUIRE(first);
    REQUIRE(wait_until([&] { return view.map(); }, 10s));
    REQUIRE(wait_until([&] { return view.control.slot_held_elsewhere(plexus::shm::k_creator_slot); }, 10s));

    std::optional<xproc_process> second = spawn_creator(view.fqn, "refused", 0, 0);
    REQUIRE(second);
    CHECK(second->wait_for(10s) == 0);
    REQUIRE(first->running());
    first->kill();
}
