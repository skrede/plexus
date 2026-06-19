#include "test_spine_lifecycle_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace spine_fixture;

TEST_CASE("integration.spine a custom observer reconstructs the ordered node/endpoint timeline",
          "[integration][inproc][spine]")
{
    fixture               fx;
    recording_observer    rec;
    const plexus::node_id id = make_id(0x0A);

    {
        inproc_node n{fx.ex, fx.disc, id, fx.ta, make_opts()};
        n.router().add_observer(rec);

        // The create edge is posted from the ctor — but the observer registered after the
        // ctor returned, so this run witnesses create via the same pump as the rest.
        {
            plexus::publisher<>  pub{n, "topic"};
            plexus::subscriber<> sub{
                    n, "topic",
                    [](std::span<const std::byte>, const plexus::io::message_info &) {}};
            fx.drive();

            // The declare + register edges surface after the pump (the handles still live).
            REQUIRE(rec.for_topic("topic").publisher_declared == 1);
            REQUIRE(rec.for_topic("topic").subscriber_registered == 1);
        }
        // The publisher handle dtor posted publisher_dropped; the subscriber handle dtor
        // drove its retire seam (subscriber_retired, last local sub). Pump to surface them.
        fx.drive();
        REQUIRE(rec.for_topic("topic").publisher_dropped == 1);
        REQUIRE(rec.for_topic("topic").subscriber_retired == 1);
    }
    // The node dtor posted participant_destroyed while the engine was still alive; the
    // executor (the fixture's) outlives the node, so pumping it now surfaces the edge —
    // pumping a node-owned executor here would be use-after-free.
    fx.drive();

    REQUIRE(rec.for_participant(id).created == 1);
    REQUIRE(rec.for_participant(id).destroyed == 1);
}

TEST_CASE("integration.spine a moved-from publisher handle fires no drop edge",
          "[integration][inproc][spine]")
{
    fixture               fx;
    recording_observer    rec;
    const plexus::node_id id = make_id(0x0B);

    inproc_node n{fx.ex, fx.disc, id, fx.ta, make_opts()};
    n.router().add_observer(rec);

    {
        plexus::publisher<> live{n, "moved"};
        // Move the live handle into another: the moved-from shell's seam ctx is nulled, so
        // ITS dtor fires nothing. Exactly one live handle remains to post a single drop.
        plexus::publisher<> taken{std::move(live)};
        fx.drive();
        REQUIRE(rec.for_topic("moved").publisher_declared == 1);
    }
    fx.drive();

    // One declare, one drop — the moved-from shell contributed neither a second declare nor
    // a second drop (the null-ctx sentinel is the sole legitimate no-fire path).
    REQUIRE(rec.for_topic("moved").publisher_dropped == 1);
}
