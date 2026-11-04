#include "test_liveliness_transport_common.h"

#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <iostream>

using namespace liveliness_transport_fixture;

namespace {

using udp_pair = node_pair<plexus::asio::udp_policy, plexus::asio::udp_transport>;

}

TEST_CASE("liveliness over udp: a connected peer is reported alive", "[node][liveliness]")
{
    udp_pair pair{"udp"};
    const clock_type::time_point t0 = pair.dial();

    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));
}

TEST_CASE("liveliness over udp: a peer that stops is reported lost within the miss window under the default policy", "[node][liveliness]")
{
    udp_pair pair{"udp"};
    const clock_type::time_point t0 = pair.dial();
    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));

    pair.close_b();
    REQUIRE(pair.pump_until([&] { return pair.lost_seen(); }, lost_bound()));
}

TEST_CASE("liveliness over udp: a peer that stops is reported lost within the miss window when heartbeats are required", "[node][liveliness]")
{
    udp_pair pair{"udp", plexus::io::combine::all_required};
    const clock_type::time_point t0 = pair.dial();
    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));
    // A heartbeat never seen does not vote, so the peer must have stamped one before it stops; three intervals carry several.
    pair.pump_until([] { return false; }, plexus::io::liveliness_options{}.heartbeat_interval * 3);

    pair.close_b();
    REQUIRE(pair.pump_until([&] { return pair.lost_seen(); }, lost_bound()));
}

TEST_CASE("liveliness over udp: time to the first alive verdict", "[.][liveliness_timing]")
{
    udp_pair pair{"udp"};
    const clock_type::time_point t0 = pair.dial();
    REQUIRE(pair.pump_until([&] { return pair.first_alive_after(t0).has_value(); }, 2s));

    std::cout << "first_alive_ms=" << pair.first_alive_after(t0)->count() << '\n';
}
