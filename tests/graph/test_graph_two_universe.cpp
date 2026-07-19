#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/default_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/discovery/universe.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

// Enumeration is universe-scoped BY CONSTRUCTION on BOTH legs (participants + topics). No enumeration
// verb takes a universe argument and none applies a query-time universe filter; a foreign-universe
// announcement is dropped at multicast_discovery::on_inbound BEFORE note_peer, so it never enters the
// awareness table, is never dialed into a session, and its topic edges — which propagate only over an
// admitted peer's session — never reach the alpha graph. A same-universe control pair proves the
// boundary is the universe compare, not a dead socket: the alpha pair rendezvous over real multicast
// AND propagates a real remote topic edge (a2's publisher lands in a1's topics_published_by), so the
// foreign peer's absence is the in-band compare, not missing traffic.
//
// The nodes listen on 0.0.0.0 (not 127.0.0.1): dial_eagerly reaches a discovered peer at the endpoint
// the announcement carries — the multicast source (egress interface) address — so a loopback-only
// listener would refuse the dial and no session would form.

namespace {

namespace pasio = plexus::asio;

using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;
using plexus::discovery::discovery_options;
using plexus::discovery::universe_scoping;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0x5a)};
    return id;
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed  = 0xC0FFEEu;
    // Dial a discovered same-universe peer eagerly so a session forms and topic edges propagate without
    // a matching local subscription. A foreign-universe peer is dropped at discovery, so never dialed.
    opts.dial_eagerly = true;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    return opts;
}

discovery_options universe_opts(std::string_view label, std::string group)
{
    discovery_options opts;
    opts.universe_pattern = std::string(label); // the label is the source of truth; the uint32 derives at construction
    opts.scoping          = universe_scoping::soft;
    opts.group            = std::move(group);
    return opts;
}

template<typename Pred>
bool pump_until(::asio::io_context &io, Pred pred, std::chrono::milliseconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
    return pred();
}

bool snapshot_sees(asio_node &node, const plexus::node_id &id)
{
    std::array<plexus::graph::participant_record, 16> buffer{};
    const auto result = node.participants(buffer);
    return std::any_of(buffer.begin(), buffer.begin() + result.count, [&](const auto &rec) { return rec.id == id; });
}

bool snapshot_sees_topic(asio_node &node, std::string_view topic)
{
    std::array<plexus::graph::topic_record, 32> buffer{};
    const auto result = node.topics(buffer);
    return std::any_of(buffer.begin(), buffer.begin() + result.count, [&](const auto &rec) { return rec.name == topic; });
}

std::size_t published_count(asio_node &node, const plexus::node_id &id)
{
    std::array<plexus::graph::topic_record, 32> buffer{};
    return node.topics_published_by(id, buffer).count;
}

}

TEST_CASE("two_universe: a foreign-universe peer and its topic stay out of enumeration while a "
          "same-universe peer's real topic edge propagates in",
          "[graph]")
{
    const auto k_bound            = std::chrono::seconds(20);
    const auto k_settle           = std::chrono::milliseconds(2500);
    const std::string base        = "239.255.0.7";
    const std::uint16_t base_port = static_cast<std::uint16_t>(24000 + (plexus::testing::process_id() % 500) * 9);
    const std::string control_topic = "control_topic";
    const std::string foreign_topic = "foreign_topic";

    ::asio::io_context io;
    pasio::default_discovery da1{io, universe_opts("alpha", base)};
    pasio::default_discovery da2{io, universe_opts("alpha", base)};
    pasio::default_discovery db{io, universe_opts("beta", base)};
    pasio::asio_transport ta1{io}, ta2{io}, tb{io};

    const auto id_a1 = make_id(0xA1);
    const auto id_a2 = make_id(0xA2);
    const auto id_b  = make_id(0xB3);
    asio_node a1{io, da1.discovery(), id_a1, ta1, make_opts()};
    asio_node a2{io, da2.discovery(), id_a2, ta2, make_opts()};
    asio_node b{io, db.discovery(), id_b, tb, make_opts()};

    a1.listen({"tcp", "0.0.0.0:" + std::to_string(base_port)});
    a2.listen({"tcp", "0.0.0.0:" + std::to_string(base_port + 1)});
    b.listen({"tcp", "0.0.0.0:" + std::to_string(base_port + 2)});

    // The same-universe peer a2 publishes a real topic that a1 subscribes; the foreign peer b publishes
    // one on its own universe. Hold all handles alive so the edges persist.
    plexus::publisher<> pub_control{a2, control_topic, plexus::topic_qos{}, true};
    plexus::subscriber<> sub_control{a1, control_topic, [](std::span<const std::byte>, const plexus::io::message_info &) {}};
    plexus::publisher<> pub_foreign{b, foreign_topic, plexus::topic_qos{}, true};

    // Positive control, participant leg: if the same-universe pair never rendezvous the host has no
    // usable multicast loopback, so every exclusion below would be a dead socket, not the compare.
    const bool participant_control = pump_until(io, [&] { return snapshot_sees(a1, id_a2) && snapshot_sees(a2, id_a1); }, k_bound);
    if(!participant_control)
        SKIP("multicast loopback unavailable on this host: the same-universe participant control reached no awareness within the bound");

    // Positive control, topic leg: drive real publishes until a2's PUBLISHER edge propagates
    // over the dialed session into a1's by-node view. This proves cross-node topic propagation is live,
    // so the foreign topic's absence below is the universe compare, not an inert propagation path.
    const std::byte payload[1]{std::byte{0x7}};
    const bool topic_control = pump_until(io, [&] { pub_control.publish(std::span<const std::byte>{payload, 1}); return published_count(a1, id_a2) >= 1; }, k_bound);
    if(!topic_control)
        SKIP("cross-node topic propagation unavailable on this host: a2's publisher edge reached no snapshot within the bound");

    // Give the beta node several announce periods on the shared base group; its datagrams ARE delivered
    // on-host and only the inbound universe compare keeps it — and any topic hanging off it — out.
    pump_until(io, [] { return false; }, k_settle);

    // Participant leg.
    REQUIRE(snapshot_sees(a1, id_a2));
    REQUIRE(snapshot_sees(a2, id_a1));
    REQUIRE_FALSE(snapshot_sees(a1, id_b));
    REQUIRE_FALSE(snapshot_sees(a2, id_b));
    REQUIRE_FALSE(snapshot_sees(b, id_a1));
    REQUIRE_FALSE(snapshot_sees(b, id_a2));

    // Topic leg — live positive control: a1 holds a2's REMOTE publisher edge (propagated over the
    // session), on both the by-topic and by-node views.
    REQUIRE(snapshot_sees_topic(a1, control_topic));
    REQUIRE(published_count(a1, id_a2) >= 1);

    // Topic leg — foreign exclusion: the foreign peer contributes zero to a1's graph despite actively
    // publishing, and symmetrically the foreign node holds none of the alpha nodes' topics.
    REQUIRE(published_count(a1, id_b) == 0);
    REQUIRE_FALSE(snapshot_sees_topic(a1, foreign_topic));
    REQUIRE(published_count(b, id_a2) == 0);
    REQUIRE_FALSE(snapshot_sees_topic(b, control_topic));
}
