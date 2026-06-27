#include "test_multicast_discovery_common.h"

#include "plexus/native/detail/announcement_card.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace multicast_discovery_fixture;

namespace {

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0xff)};
    return id;
}

std::vector<plexus::discovery::listening_transport> sample_listens()
{
    return {{"tcp", 7000}, {"udp", 7100}};
}

std::vector<std::byte> announce_bytes(std::uint8_t seed)
{
    return plexus::wire::encode_announcement(plexus::native::detail::announcement_from_card(make_id(seed), sample_listens(), 30, 0));
}

std::vector<std::byte> goodbye_bytes(std::uint8_t seed)
{
    return plexus::wire::encode_announcement(
            plexus::native::detail::announcement_from_card(make_id(seed), sample_listens(), 30, plexus::wire::k_announcement_goodbye_flag));
}

plexus::native::discovery_options capped_options(std::size_t max_peers, std::size_t per_source_max, bool evict_lru)
{
    plexus::native::discovery_options opts;
    opts.cap.max_peers      = max_peers;
    opts.cap.per_source_max = per_source_max;
    opts.cap.evict_lru      = evict_lru;
    return opts;
}

} // namespace

TEST_CASE("multicast_discovery: a flood of distinct sources admits at most the cap", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(3, 0, false)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    for(std::uint8_t i = 0; i < 10; ++i)
    {
        fake_clock::advance(std::chrono::milliseconds(1));
        socket.inject("10.0.0." + std::to_string(i), announce_bytes(i));
    }

    REQUIRE(resolved == 3);
}

TEST_CASE("multicast_discovery: an established peer still resolves after the cap fills with others", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(2, 0, false)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.1", announce_bytes(1)); // admitted, cap now 1/2
    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.2", announce_bytes(2)); // admitted, cap now 2/2 (full)
    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.3", announce_bytes(3)); // new source past cap, dropped
    REQUIRE(resolved == 2);

    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.1", announce_bytes(1)); // established, must still resolve

    REQUIRE(resolved == 3);
}

TEST_CASE("multicast_discovery: with evict_lru the newest source replaces the oldest", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(2, 0, true)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.1", announce_bytes(1));
    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.2", announce_bytes(2));
    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.3", announce_bytes(3)); // evicts the oldest (10.0.0.1) and resolves

    REQUIRE(resolved == 3);
}

TEST_CASE("multicast_discovery: a single source past the per-source rate is dropped within the window", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(0, 2, false)}; // unlimited cap, 2 per 1s window

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    socket.inject("10.0.0.1", announce_bytes(1)); // 1st, admit
    fake_clock::advance(std::chrono::milliseconds(100));
    socket.inject("10.0.0.1", announce_bytes(1)); // 2nd within window, admit (allowance 2)
    fake_clock::advance(std::chrono::milliseconds(100));
    socket.inject("10.0.0.1", announce_bytes(1)); // 3rd within window, dropped
    REQUIRE(resolved == 2);

    fake_clock::advance(std::chrono::milliseconds(1000)); // window rolls over
    socket.inject("10.0.0.1", announce_bytes(1)); // fresh window, admit

    REQUIRE(resolved == 3);
}

TEST_CASE("multicast_discovery: with the cap disabled every distinct source resolves", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(0, 0, false)}; // unlimited + no rate limit

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    for(std::uint8_t i = 0; i < 50; ++i)
    {
        fake_clock::advance(std::chrono::milliseconds(1));
        socket.inject("10.1.0." + std::to_string(i), announce_bytes(i));
    }

    REQUIRE(resolved == 50);
}

TEST_CASE("multicast_discovery: the cap does not gate the goodbye path", "[multicast_discovery]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket, capped_options(1, 1, false)}; // cap of 1, fills then never admits a new source

    int resolved  = 0;
    int withdrawn = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    disc.on_withdrawn([&](const plexus::discovery::service_info &) { ++withdrawn; });

    fake_clock::advance(std::chrono::milliseconds(1));
    socket.inject("10.0.0.1", announce_bytes(1)); // fills the cap (1/1)
    REQUIRE(resolved == 1);

    // A flood of goodbyes from never-admitted sources must all fire on_withdrawn regardless of cap/rate.
    for(std::uint8_t i = 100; i < 110; ++i)
    {
        fake_clock::advance(std::chrono::milliseconds(1));
        socket.inject("10.0.0." + std::to_string(i), goodbye_bytes(i));
    }

    REQUIRE(withdrawn == 10);
    REQUIRE(resolved == 1);
}
