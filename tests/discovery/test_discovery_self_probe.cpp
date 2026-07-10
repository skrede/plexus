#include "test_multicast_discovery_common.h"

#include "plexus/asio/default_discovery.h"

#include "plexus/discovery/detail/announcement_card.h"
#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/wire/announcement.h"

#include "plexus/io/network_interface.h"
#include "plexus/log/logger.h"
#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <string_view>
#include <vector>
#include <chrono>
#include <cstdint>

using namespace multicast_discovery_fixture;

namespace {

using plexus::discovery::discovery_health;

class capturing_logger final : public plexus::log::logger
{
public:
    void warn(std::string_view message) override
    {
        messages.emplace_back(message);
    }

    std::vector<std::string> messages;
};

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

plexus::discovery::service_info card_for(const plexus::node_id &id)
{
    return {"node", plexus::io::endpoint{"", "host"}, plexus::discovery::assemble_contact_card(id, sample_listens())};
}

} // namespace

TEST_CASE("discovery_self_probe: a node never notes its own echo and reads healthy on seeing it", "[multicast_discovery][discovery_self_probe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket};

    const auto self_id = make_id(0x42);
    disc.advertise(card_for(self_id));
    REQUIRE(socket.sent.size() == 1);

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    const auto window = std::chrono::milliseconds{100};
    REQUIRE(disc.probe(std::chrono::steady_clock::now(), window) == discovery_health::not_yet);

    // The node's OWN emitted datagram delivered back over loopback: excluded, never resolved.
    socket.inject("127.0.0.1", socket.sent.front());
    REQUIRE(resolved == 0);
    REQUIRE(disc.probe(std::chrono::steady_clock::now(), window) == discovery_health::healthy);

    // A distinct peer id on the same group still resolves — exclusion is by node_id, not source.
    const auto peer = plexus::wire::encode_announcement(plexus::discovery::detail::announcement_from_card(make_id(0x99), sample_listens(), 30, 0));
    socket.inject("10.0.0.2", peer);
    REQUIRE(resolved == 1);
}

TEST_CASE("discovery_self_probe: the probe reaches no_self_echo once the window elapses unseen", "[multicast_discovery][discovery_self_probe]")
{
    fake_clock::reset();
    int ex = 0;
    fake_datagram_socket socket;
    discovery_capped disc{ex, socket};

    disc.advertise(card_for(make_id(0x11)));
    disc.browse([&](const plexus::discovery::service_info &) {});

    const auto window = std::chrono::milliseconds{5000};
    REQUIRE(disc.probe(fake_clock::now(), window) == discovery_health::not_yet);

    fake_clock::advance(std::chrono::milliseconds{5001});
    REQUIRE(disc.probe(fake_clock::now(), window) == discovery_health::no_self_echo);
}

TEST_CASE("discovery_self_probe: default_discovery over a bogus interface reads bad_interface and warns once", "[discovery_self_probe]")
{
    ::asio::io_context io;
    capturing_logger log;

    plexus::discovery::discovery_options options;
    options.egress_interface = plexus::io::network_interface::by_name("plexus-nonexistent-iface-xyz0");

    plexus::asio::default_discovery disc{io, options, log};

    REQUIRE(disc.self_check() == discovery_health::bad_interface);
    REQUIRE(disc.self_check() == discovery_health::bad_interface);
    REQUIRE(log.messages.size() == 1);
}

TEST_CASE("discovery_self_probe: default_discovery with no self-echo reaches no_self_echo and warns once", "[discovery_self_probe]")
{
    ::asio::io_context io;
    capturing_logger log;

    plexus::discovery::discovery_options options;
    options.announce_period = std::chrono::milliseconds{1}; // a small probe window; io is never pumped

    plexus::asio::default_discovery disc{io, options, log};

    discovery_health health   = discovery_health::not_yet;
    const auto bound          = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(std::chrono::steady_clock::now() < bound)
    {
        health = disc.self_check();
        if(health != discovery_health::not_yet)
            break;
    }

    REQUIRE(health == discovery_health::no_self_echo);
    REQUIRE(log.messages.size() == 1);
}

// Hidden [.multicast]: real same-host multicast loopback is environment-sensitive (a sandbox may drop
// IGMP-joined delivery), so the deterministic cases above carry the exclusion/health assertions and
// this on-demand case proves the same end-to-end through the factory. It is recorded, never silently
// passed: `ctest -R discovery_self_probe.[.multicast]` runs it where loopback is available.
TEST_CASE("discovery_self_probe: a live node over default_discovery excludes itself and reads healthy", "[.multicast][discovery_self_probe]")
{
    ::asio::io_context io;
    plexus::discovery::discovery_options options;
    options.announce_period = std::chrono::milliseconds{20};

    plexus::asio::default_discovery disc{io, options};

    const auto self_id = make_id(0x7e);
    int self_resolved  = 0;
    disc.discovery().browse(
            [&](const plexus::discovery::service_info &peer)
            {
                if(plexus::discovery::assemble_contact_card(self_id, sample_listens()) == peer.metadata)
                    ++self_resolved;
            });
    disc.discovery().advertise(card_for(self_id));

    const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while(disc.self_check() != discovery_health::healthy && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }

    REQUIRE(disc.self_check() == discovery_health::healthy);
    REQUIRE(self_resolved == 0);
}
