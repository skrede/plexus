#include "test_multicast_discovery_common.h"

#include "plexus/native/detail/announcement_card.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>

using namespace multicast_discovery_fixture;

namespace {

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0xff)};
    return id;
}

std::vector<plexus::discovery::listening_transport> sample_listens()
{
    return {{"tcp", 7000}, {"udp", 7100}};
}

} // namespace

TEST_CASE("multicast_discovery: service_info metadata is byte-identical to assemble_contact_card", "[multicast_discovery]")
{
    const auto id      = make_id(0x42);
    const auto listens = sample_listens();

    plexus::wire::announcement ann = plexus::native::detail::announcement_from_card(id, listens, 30, 0);
    const auto info                = plexus::native::detail::service_info_from_announcement(ann, "192.168.1.5");

    const auto reference = plexus::discovery::assemble_contact_card(id, listens);
    REQUIRE(info.metadata == reference);
    REQUIRE(info.endpoint.address == "192.168.1.5");
}

TEST_CASE("multicast_discovery: advertise emits an announcement immediately on join", "[multicast_discovery]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    const auto id   = make_id(0x11);
    const auto card = plexus::discovery::assemble_contact_card(id, sample_listens());
    disc.advertise({"node", plexus::io::endpoint{"", "host"}, card});

    REQUIRE(socket.sent.size() == 1);
    const auto decoded = plexus::wire::decode_announcement(socket.sent.front());
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->node_id == id);
}

TEST_CASE("multicast_discovery: a foreign or truncated datagram fires no on_resolved", "[multicast_discovery]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    const std::vector<std::byte> bad_magic{std::byte{'X'}, std::byte{'X'}, std::byte{'X'}, std::byte{'X'}, std::byte{0}, std::byte{1}};
    socket.inject("10.0.0.9", bad_magic);

    const auto id    = make_id(0x33);
    const auto good  = plexus::wire::encode_announcement(plexus::native::detail::announcement_from_card(id, sample_listens(), 30, 0));
    const std::vector<std::byte> truncated{good.begin(), good.begin() + 5};
    socket.inject("10.0.0.9", truncated);

    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: a valid datagram resolves with the datagram source host", "[multicast_discovery]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    std::optional<plexus::discovery::service_info> got;
    disc.browse([&](const plexus::discovery::service_info &s) { got = s; });

    const auto id   = make_id(0x55);
    const auto good = plexus::wire::encode_announcement(plexus::native::detail::announcement_from_card(id, sample_listens(), 30, 0));
    socket.inject("172.16.0.4", good);

    REQUIRE(got.has_value());
    REQUIRE(got->endpoint.address == "172.16.0.4");
    REQUIRE(got->metadata == plexus::discovery::assemble_contact_card(id, sample_listens()));
}
