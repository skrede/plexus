#include "test_spine_lifecycle_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace spine_fixture;

TEST_CASE("integration.spine the wire unsubscribe (refcount 1->0) surfaces on_qos_change "
          "unsubscribed",
          "[integration][inproc][spine]")
{
    pair_fixture       fx;
    recording_observer rec;
    fx.b.router().add_observer(rec); // the subscriber node owns the demand-side wire edge
    fx.connect();
    REQUIRE(fx.b.router().is_connected(fx.id_a));

    {
        // The subscriber lives on B; its demand emits a wire subscribe toward A (the
        // demand-side attach). Retiring it (scope exit) is the last local sub, so B's
        // session detach fires the wire unsubscribe and its 1->0 on_qos_change edge.
        plexus::subscriber<> sub{fx.b, "wire-topic", [](std::span<const std::byte>, const plexus::io::message_info &) {}};
        fx.drive();
        REQUIRE(rec.unsubscribed_for("wire-topic") == 0);
    }
    fx.drive();

    // The 1->0 detach fired the wire unsubscribe edge (at least once — a peer reachable
    // over more than one resolved route detaches per route).
    REQUIRE(rec.unsubscribed_for("wire-topic") >= 1);
}
