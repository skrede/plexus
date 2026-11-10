#include "test_liveliness_transport_common.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <iostream>

using namespace liveliness_transport_fixture;

namespace {

using tcp_pair = node_pair<plexus::asio::asio_policy, plexus::asio::asio_transport>;

}

TEST_CASE("liveliness over tcp: a connected peer is reported alive", "[node][liveliness]")
{
    tcp_pair pair{"tcp"};
    const clock_type::time_point t0 = pair.dial();

    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));
}

TEST_CASE("liveliness over tcp: a peer that stops is reported lost within the miss window", "[node][liveliness]")
{
    tcp_pair pair{"tcp"};
    const clock_type::time_point t0 = pair.dial();
    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));

    pair.close_b();
    REQUIRE(pair.pump_until([&] { return pair.lost_seen(); }, lost_bound()));
}

TEST_CASE("liveliness over tcp: time to the first alive verdict", "[.][liveliness_timing]")
{
    tcp_pair pair{"tcp"};
    const clock_type::time_point t0 = pair.dial();
    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));

    std::cout << "first_alive_ms=" << pair.first_alive_after(t0)->count() << '\n';
}
