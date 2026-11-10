#include "test_inproc_stamp_demand_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stamp_demand_fixture;

TEST_CASE("inproc stamp demand: the per-topic latch OR-reduces over local subscriber demand", "[integration][inproc][stamp]")
{
    using registry = plexus::io::subscriber_registry<stub_channel>;
    registry reg;
    const auto hash = plexus::wire::fqn_topic_hash("topic");

    // An unknown topic latches true (the safe always-on default — it stamps).
    REQUIRE(reg.any_subscriber_wants_info(hash));

    // A lone no-info subscriber (the 2-arg arity) collapses the latch to false.
    stub_channel c_no;
    plexus::io::subscriber_qos qos_no;
    qos_no.wants_message_info = false;
    reg.add_subscriber(hash, "topic", c_no, "node-no", qos_no);
    REQUIRE_FALSE(reg.any_subscriber_wants_info(hash));

    // A second, info-wanting subscriber forces stamping for the whole topic (OR-reduce):
    // a suppressing subscriber never starves a demanding one.
    stub_channel c_yes;
    plexus::io::subscriber_qos qos_yes; // wants_message_info defaults true
    reg.add_subscriber(hash, "topic", c_yes, "node-yes", qos_yes);
    REQUIRE(reg.any_subscriber_wants_info(hash));

    // Retiring the demander recomputes the latch back to the lone no-info state.
    reg.remove_subscriber(hash, c_yes);
    REQUIRE_FALSE(reg.any_subscriber_wants_info(hash));

    // Retiring the last subscriber returns the topic to the empty-set default true.
    reg.remove_subscriber(hash, c_no);
    REQUIRE(reg.any_subscriber_wants_info(hash));
}
