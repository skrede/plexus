#include "test_multicast_discovery_common.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/discovery_options.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <stdexcept>

using namespace multicast_discovery_fixture;
using namespace std::chrono_literals;

namespace {

plexus::wire::announcement announced_with(plexus::discovery::discovery_options options)
{
    plexus::node_id id{};
    id[0] = std::byte{0x5a};

    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, std::move(options)};
    disc.advertise({"node", plexus::io::endpoint{"", "host"}, plexus::discovery::assemble_contact_card(id, {{"tcp", 7000}})});

    REQUIRE(socket.sent.size() == 1);
    const auto decoded = plexus::wire::decode_announcement(socket.sent.front());
    REQUIRE(decoded.has_value());
    return *decoded;
}

plexus::discovery::discovery_options with_ttl(std::chrono::milliseconds period, std::chrono::seconds ttl)
{
    plexus::discovery::discovery_options options;
    options.announce_period = period;
    options.announce_ttl    = ttl;
    return options;
}

void construct(plexus::discovery::discovery_options options)
{
    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, std::move(options)};
}

}

TEST_CASE("multicast_discovery: an announcement from default options carries the default awareness horizon as its ttl", "[multicast_discovery][announce_ttl]")
{
    REQUIRE(announced_with({}).ttl_secs == 15);
}

TEST_CASE("multicast_discovery: the announced ttl follows announce_ttl and not the hop limit", "[multicast_discovery][announce_ttl]")
{
    plexus::discovery::discovery_options hops;
    hops.hop_limit = 1;
    REQUIRE(announced_with(hops).ttl_secs == 15);

    REQUIRE(announced_with(with_ttl(1000ms, 30s)).ttl_secs == 30);
}

TEST_CASE("multicast_discovery: construction refuses an announce ttl that does not exceed the announce period", "[multicast_discovery][announce_ttl]")
{
    CHECK_THROWS_AS(construct(with_ttl(1000ms, 1s)), std::runtime_error);
    CHECK_THROWS_AS(construct(with_ttl(1000ms, 0s)), std::runtime_error);
    CHECK_THROWS_AS(construct(with_ttl(20s, 15s)), std::runtime_error);

    CHECK_NOTHROW(construct(with_ttl(1999ms, 2s)));
}

TEST_CASE("multicast_discovery: construction refuses a non-positive announce period or announce ttl", "[multicast_discovery][announce_ttl]")
{
    CHECK_THROWS_AS(construct(with_ttl(0ms, 15s)), std::runtime_error);
    CHECK_THROWS_AS(construct(with_ttl(-1000ms, 15s)), std::runtime_error);
    CHECK_THROWS_AS(construct(with_ttl(-2000ms, 0s)), std::runtime_error);
    CHECK_THROWS_AS(construct(with_ttl(-2000ms, -1s)), std::runtime_error);

    CHECK_NOTHROW(construct(with_ttl(1ms, 1s)));
}
