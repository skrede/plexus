#include "test_multicast_discovery_common.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/discovery/detail/announcement_card.h"
#include "plexus/discovery/detail/discovery_flood_cap.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace multicast_discovery_fixture;

namespace {

struct aging_clock
{
    using duration   = std::chrono::milliseconds;
    using time_point = std::chrono::time_point<aging_clock, duration>;
};

using aging_cap = plexus::discovery::detail::discovery_flood_cap<aging_clock>;

aging_cap make_cap(std::size_t max_peers)
{
    plexus::discovery::flood_cap_options options;
    options.max_peers = max_peers;
    return aging_cap{options};
}

std::string source(std::uint32_t index)
{
    return "10.0.0." + std::to_string(index);
}

std::vector<std::byte> announce_bytes(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<std::uint8_t>(seed ^ 0xff)};
    const std::vector<plexus::discovery::listening_transport> listens{{"tcp", 7000}, {"udp", 7100}};
    return plexus::wire::encode_announcement(plexus::discovery::detail::announcement_from_card(id, listens, 30, 0));
}

}

TEST_CASE("flood cap: an admitted source silent for longer than idle_expiry ages out and frees its slot", "[flood_cap]")
{
    aging_cap cap = make_cap(3);
    aging_clock::time_point now{};
    for(std::uint32_t i = 0; i < 3; ++i)
        REQUIRE(cap.admit(source(i), now));

    now += std::chrono::seconds(16);

    REQUIRE(cap.admit(source(3), now));
    REQUIRE(cap.size() <= 3);
}

TEST_CASE("flood cap: a source that keeps announcing does not age out", "[flood_cap]")
{
    aging_cap cap = make_cap(3);
    aging_clock::time_point now{};
    for(std::uint32_t i = 0; i < 3; ++i)
        REQUIRE(cap.admit(source(i), now));

    for(std::uint32_t elapsed = 5; elapsed <= 20; elapsed += 5)
    {
        now = aging_clock::time_point{} + std::chrono::seconds(elapsed);
        REQUIRE(cap.admit(source(0), now));
    }

    REQUIRE(cap.admit(source(3), now));
    const std::size_t before = cap.size();
    REQUIRE(cap.admit(source(0), now + std::chrono::seconds(1)));
    REQUIRE(cap.size() == before);
    REQUIRE(cap.size() <= 3);
}

TEST_CASE("flood cap: a full cap of fresh sources still refuses a new one", "[flood_cap]")
{
    aging_cap cap = make_cap(3);
    aging_clock::time_point now{};
    for(std::uint32_t i = 0; i < 3; ++i)
        REQUIRE(cap.admit(source(i), now));

    now += std::chrono::seconds(1);

    REQUIRE_FALSE(cap.admit(source(3), now));
    REQUIRE(cap.size() == 3);
}

TEST_CASE("multicast_discovery: a node is still discovered after max_peers sources have come and gone", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    plexus::discovery::discovery_options options;
    options.cap.max_peers = 3;
    discovery_capped disc{ex, socket, options};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    for(std::uint8_t i = 0; i < 3; ++i)
    {
        fake_clock::advance(std::chrono::milliseconds(1));
        socket.inject(source(i), announce_bytes(i));
    }
    REQUIRE(resolved == 3);

    fake_clock::advance(std::chrono::seconds(16));
    socket.inject(source(3), announce_bytes(3));

    REQUIRE(resolved == 4);
}
