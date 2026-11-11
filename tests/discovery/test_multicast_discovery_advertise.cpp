#include "test_multicast_discovery_common.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using namespace multicast_discovery_fixture;

namespace {

using card_metadata = std::vector<std::pair<std::string, std::string>>;
using listen_list   = std::vector<std::pair<std::string, std::uint16_t>>;
using datagrams     = std::vector<std::vector<std::byte>>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<std::uint8_t>(seed ^ 0xff)};
    return id;
}

std::string hex_of(std::uint8_t seed)
{
    return plexus::discovery::detail::hex_encode(make_id(seed));
}

plexus::discovery::service_info card_of(card_metadata metadata)
{
    return {"node", plexus::io::endpoint{"", "host"}, std::move(metadata)};
}

card_metadata numbered_card(std::int32_t count)
{
    card_metadata card{{"node_id", hex_of(0x21)}};
    for(std::int32_t i = 0; i < count; ++i)
        card.emplace_back(plexus::discovery::detail::port_key("t" + std::to_string(i)), std::to_string(1000 + i));
    return card;
}

listen_list numbered_listens(std::int32_t count)
{
    listen_list listens;
    for(std::int32_t i = 0; i < count; ++i)
        listens.emplace_back("t" + std::to_string(i), static_cast<std::uint16_t>(1000 + i));
    return listens;
}

card_metadata named_card(const std::string &transport)
{
    return {{"node_id", hex_of(0x22)}, {plexus::discovery::detail::port_key(transport), "7000"}};
}

datagrams sent_on_advertise(card_metadata metadata)
{
    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};
    disc.advertise(card_of(std::move(metadata)));
    return socket.sent;
}

plexus::wire::announcement only_announcement(const datagrams &sent)
{
    REQUIRE(sent.size() == 1);
    const auto decoded = plexus::wire::decode_announcement(sent.front());
    REQUIRE(decoded.has_value());
    return *decoded;
}

}

TEST_CASE("multicast_discovery: a card whose node id is not its first entry is announced with that node id", "[multicast_discovery]")
{
    const auto ann = only_announcement(sent_on_advertise({{"plexus/tcp/port", "7000"}, {"node_id", hex_of(0x11)}, {"plexus/schema", "1"}}));

    REQUIRE(ann.node_id == make_id(0x11));
    REQUIRE(ann.listens == listen_list{{"tcp", 7000}});
}

TEST_CASE("multicast_discovery: a card with no decodable node id announces nothing and sends no goodbye on stop", "[multicast_discovery]")
{
    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    disc.advertise(card_of({{"plexus/tcp/port", "7000"}}));
    disc.stop();

    REQUIRE(socket.sent.empty());
}

TEST_CASE("multicast_discovery: a refused re-advertise leaves the last advertised card in place", "[multicast_discovery]")
{
    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    disc.advertise(card_of(plexus::discovery::assemble_contact_card(make_id(0x33), {{"tcp", 7000}})));
    REQUIRE(socket.sent.size() == 1);
    disc.advertise(card_of({{"plexus/tcp/port", "7000"}}));
    REQUIRE(socket.sent.size() == 1);

    disc.stop();

    REQUIRE(socket.sent.size() == 2);
    const auto bye = plexus::wire::decode_announcement(socket.sent.back());
    REQUIRE(bye.has_value());
    REQUIRE((bye->flags & plexus::wire::k_announcement_goodbye_flag) != 0);
    REQUIRE(bye->node_id == make_id(0x33));
}

TEST_CASE("multicast_discovery: a card port value with trailing characters announces no listen for it", "[multicast_discovery]")
{
    const auto ann = only_announcement(sent_on_advertise({{"node_id", hex_of(0x44)}, {"plexus/tcp/port", "7000x"}, {"plexus/udp/port", "7100"}}));

    REQUIRE(ann.listens == listen_list{{"udp", 7100}});
}

TEST_CASE("multicast_discovery: a card whose listens overflow the announcement's one-byte count or name length announces nothing", "[multicast_discovery]")
{
    REQUIRE(sent_on_advertise(numbered_card(256)).empty());
    REQUIRE(sent_on_advertise(named_card(std::string(256, 'n'))).empty());

    REQUIRE(only_announcement(sent_on_advertise(numbered_card(255))).listens == numbered_listens(255));
    REQUIRE(only_announcement(sent_on_advertise(named_card(std::string(255, 'n')))).listens == listen_list{{std::string(255, 'n'), 7000}});
}

TEST_CASE("multicast_discovery: advertise reports whether it advertised the card", "[multicast_discovery]")
{
    std::int32_t ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    REQUIRE_FALSE(disc.advertise(card_of({{"plexus/tcp/port", "7000"}})));
    REQUIRE_FALSE(disc.advertise(card_of(numbered_card(256))));
    REQUIRE(disc.advertise(card_of(plexus::discovery::assemble_contact_card(make_id(0x66), {{"tcp", 7000}}))));
    REQUIRE_FALSE(disc.advertise(card_of({{"plexus/tcp/port", "7000"}})));
    REQUIRE(socket.sent.size() == 1);
}
