#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/minter_entry.h"
#include "support/xproc_child_main.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#if !defined(_WIN32)
    #include "support/sidecar_probe.h"

    #include <fcntl.h>

    #include <sys/mman.h>
#endif

// A re-exec'd child leaves what a creator killed in the middle of a mint leaves and exits without releasing anything;
// the next producer must still mint the ring, and a reader must receive from it. On Windows a name dies with its last
// handle, so nothing is left there to give way.

namespace {

namespace pio = plexus::shm;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::lifetime_fqn;
using plexus::testing::native_broker;

constexpr std::int32_t k_rounds = 100;

bool create_region(native_broker &broker, const std::string &name, std::size_t bytes, native_broker::region_handle &out)
{
    return broker.create(name, bytes, pio::create_options{}, out) == pio::region_status::ok;
}

#if !defined(_WIN32)
// A minter killed between its slab's exclusive open and its sizing.
bool open_unsized(const std::vector<std::string> &args)
{
    return args.size() > 2 && ::shm_open(args[2].c_str(), O_CREAT | O_EXCL | O_RDWR, 0600) >= 0;
}
#else
bool open_unsized(const std::vector<std::string> &)
{
    return false;
}
#endif

// The leftover kind comes first: "ring" leaves a control region and its slab, "unsized" a control region and a slab name
// never sized, "slab" a slab alone. The control name comes second.
bool leave_leftover(const std::vector<std::string> &args)
{
    if(args.size() < 2)
        return false;
    const pio::ring_geometry geom = plexus::testing::minter_geometry();
    native_broker broker;
    native_broker::region_handle control;
    native_broker::region_handle slab;
    if(args[0] != "slab" && !create_region(broker, args[1], pio::control_region_bytes(geom.cell_count), control))
        return false;
    const std::size_t slab_bytes = pio::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    const bool left              = args[0] == "unsized" ? open_unsized(args) : create_region(broker, args[1] + ".s", slab_bytes, slab);
    if(!left)
        return false;
    std::_Exit(0);
}

[[maybe_unused]] const bool s_leftover_registered = plexus::testing::register_xproc_child("shm.stale_reclaim.leftover", &leave_leftover);

void reclaim_each_round(ring_view &view, std::vector<std::string> args)
{
    wake_bus bus;
    for(std::int32_t round = 0; round < k_rounds; ++round)
    {
        const auto outcome = plexus::testing::run_xproc("shm.stale_reclaim.leftover", args,
                                                        [&]
                                                        {
                                                            plexus::testing::engage_afresh(bus, view);
                                                            return true;
                                                        });
        REQUIRE(outcome.child_succeeded);
        REQUIRE(outcome.parent_succeeded);
    }
}

}

TEST_CASE("shm.stale_reclaim an unstamped control region and its slab give way to the next producer", "[shm][stale_reclaim]")
{
    ring_view view{lifetime_fqn("stale_reclaim.ring")};
    reclaim_each_round(view, {"ring", view.name});
}

#if !defined(_WIN32)
TEST_CASE("shm.stale_reclaim an unstamped control region and a slab never sized give way to the next producer", "[shm][stale_reclaim]")
{
    ring_view view{lifetime_fqn("stale_reclaim.unsized")};
    reclaim_each_round(view, {"unsized", view.name, plexus::testing::canonical_of(view.name + ".s")});
}
#endif

TEST_CASE("shm.stale_reclaim a slab left without its control region gives way to the next producer", "[shm][stale_reclaim]")
{
    ring_view view{lifetime_fqn("stale_reclaim.slab")};
    reclaim_each_round(view, {"slab", view.name});
}

int main(int argc, char **argv)
{
    plexus::testing::xproc_capture_argv(argc, argv);
    plexus::testing::xproc_child_main(argc, argv);
    return Catch::Session().run(argc, argv);
}
