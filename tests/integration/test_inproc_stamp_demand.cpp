#include "test_inproc_stamp_demand_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stamp_demand_fixture;

TEST_CASE("inproc stamp demand: a 3-arg subscriber with default qos sees populated stamps",
          "[integration][inproc][stamp]")
{
    net n;
    n.connect();

    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic",
                       [&](const sample &, const message_info &info) { infos.push_back(info); }};
    typed_publisher  p{n.b, "topic", plexus::typed_publisher_options{}, counting_codec{}};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x1234u;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(infos.size() == 1);
    REQUIRE(infos.front().source_timestamp != 0);
    REQUIRE(infos.front().reception_timestamp != 0);
    REQUIRE(infos.front().from_intra_process);
}

TEST_CASE("inproc stamp demand: a 3-arg subscriber that opts out sees a documented 0 stamp",
          "[integration][inproc][stamp]")
{
    net n;
    n.connect();

    plexus::typed_subscriber_options opts;
    opts.qos.wants_message_info = false; // an informed 3-arg opt-out: deliver no timestamps

    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic", opts,
                       [&](const sample &, const message_info &info) { infos.push_back(info); }};
    typed_publisher  p{n.b, "topic", plexus::typed_publisher_options{}, counting_codec{}};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x5678u;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(infos.size() == 1);
    REQUIRE(infos.front().source_timestamp == 0); // documented "not stamped"
    REQUIRE(infos.front().reception_timestamp == 0);
    REQUIRE(infos.front().from_intra_process); // arity-independent, always honest
}
