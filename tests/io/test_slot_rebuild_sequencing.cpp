#include "slot_rebuild_fixture.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>

using plexus::testing::slot_channel;
using plexus::testing::slot_rebuild_fixture;

namespace {

const std::vector<std::string> k_retired_in_order{"notified", "destroyed"};

}

TEST_CASE("slot_rebuild_sequencing a handshake completion for the current session still times it out", "[io][slot_rebuild_sequencing]")
{
    slot_rebuild_fixture fx;
    fx.hand();
    const std::int32_t initial = fx.probe(0).sends;

    fx.executor.timer().manufacture_expiry();
    fx.executor.run_posted();
    REQUIRE(fx.probe(0).sends == initial + 1);
    REQUIRE_FALSE(fx.probe(0).closed);

    fx.executor.timer().manufacture_expiry();
    fx.executor.run_posted();
    REQUIRE(fx.probe(0).closed);
}

TEST_CASE("slot_rebuild_sequencing a handshake completion that expired before its slot was rebuilt does nothing to the rebuilt session", "[io][slot_rebuild_sequencing]")
{
    slot_rebuild_fixture fx;
    fx.hand();
    const std::int32_t initial = fx.probe(0).sends;

    fx.executor.timer().manufacture_expiry();
    fx.registry.session_for(fx.peer)->tear_down();
    fx.hand();
    fx.executor.run_posted();
    REQUIRE(fx.registry.has_session(fx.peer));
    REQUIRE(fx.probe(1).sends == initial);
    REQUIRE_FALSE(fx.probe(1).closed);

    fx.executor.timer().manufacture_expiry();
    fx.executor.run_posted();
    REQUIRE(fx.probe(1).sends == initial + 1);
    REQUIRE_FALSE(fx.probe(1).closed);
}

TEST_CASE("slot_rebuild_sequencing a rebuild never destroys the old channel before its close notification", "[io][slot_rebuild_sequencing]")
{
    slot_rebuild_fixture fx;
    fx.hand();
    fx.registry.session_for(fx.peer)->tear_down();
    fx.hand();
    fx.executor.run_posted();

    REQUIRE(fx.probe(0).events == k_retired_in_order);
    REQUIRE(fx.registry.has_session(fx.peer));
    REQUIRE_FALSE(fx.probe(1).closed);
}

TEST_CASE("slot_rebuild_sequencing a rebuild parked behind an unsettled old session resumes once its wait drained", "[io][slot_rebuild_sequencing]")
{
    slot_rebuild_fixture fx;
    fx.hand();
    const std::int32_t initial = fx.probe(0).sends;

    fx.executor.timer().manufacture_expiry();
    fx.registry.session_for(fx.peer)->tear_down();
    fx.hand();
    REQUIRE(fx.probe(1).sends == 0);
    REQUIRE(fx.probe(0).events.empty());

    fx.executor.run_posted();
    REQUIRE(fx.probe(1).sends == initial);
    REQUIRE(fx.probe(0).events == k_retired_in_order);
}

TEST_CASE("slot_rebuild_sequencing a rebuild parked behind a retired channel still quiescing resumes once it was erased", "[io][slot_rebuild_sequencing]")
{
    slot_rebuild_fixture fx;
    slot_channel &first        = fx.hand();
    const std::int32_t initial = fx.probe(0).sends;
    first.hold();

    fx.registry.session_for(fx.peer)->tear_down();
    fx.hand();
    fx.registry.session_for(fx.peer)->tear_down();
    fx.hand();
    fx.executor.run_posted();
    REQUIRE(fx.probe(0).events.empty());
    REQUIRE(fx.probe(2).sends == 0);

    first.release();
    fx.executor.run_posted();
    REQUIRE(fx.probe(0).events == k_retired_in_order);
    REQUIRE(fx.probe(1).events == k_retired_in_order);
    REQUIRE(fx.probe(2).sends == initial);
    REQUIRE_FALSE(fx.probe(2).closed);
}
