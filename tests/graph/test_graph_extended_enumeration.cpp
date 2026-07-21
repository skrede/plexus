// The extended-world enumeration facade: a reported peer surfaces in participants()/topics() shown
// AS relayed — reported-by and via-whom visible — while a genuinely direct peer's record in the same
// snapshot keeps the direct-only shape. The two record shapes coexist, never blended.

#include "test_graph_node_common.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/peer_report.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using namespace graph_node_fixture;
namespace wire = plexus::wire;

namespace {

const plexus::graph::participant_record *record_for(std::span<const plexus::graph::participant_record> filled, const plexus::node_id &id)
{
    const auto it = std::find_if(filled.begin(), filled.end(), [&](const auto &rec) { return rec.id == id; });
    return it == filled.end() ? nullptr : &*it;
}

const plexus::graph::topic_record *topic_for(std::span<const plexus::graph::topic_record> filled, const plexus::node_id &node)
{
    const auto it = std::find_if(filled.begin(), filled.end(), [&](const auto &rec) { return rec.node == node; });
    return it == filled.end() ? nullptr : &*it;
}

wire::peer_report reported_origin(const plexus::node_id &origin, std::string_view fqn, std::string_view type_name)
{
    wire::peer_report pr;
    pr.origin          = origin;
    pr.origin_universe = plexus::discovery::k_default_universe;
    pr.hop             = 1;
    pr.seq             = 1;
    pr.flags           = wire::k_peer_report_consent_flag;
    pr.topics.push_back(wire::topic_declaration{0, 42, std::string{fqn}, std::string{type_name}, wire::type_state::declared});
    return pr;
}

}

TEST_CASE("participants surfaces a reported peer as relayed beside an unblended direct peer", "[graph]")
{
    host        h;
    inproc_node node{h.ex, h.disc, make_id(0x01), h.transport, make_opts()};

    const auto id_direct = make_id(0x0B);
    const auto id_relay  = make_id(0x0A);
    const auto id_origin = make_id(0x0C);
    const auto ep_direct = make_ep("host-b:6000");
    const auto ep_relay  = make_ep("host-a:5000");
    node.router().note_peer(id_direct, ep_direct);
    node.router().note_peer(id_relay, ep_relay);
    node.router().ingest_peer_report(id_relay, reported_origin(id_origin, "sensor/temp", "Temp"));

    std::array<plexus::graph::participant_record, 8> buffer{};
    const auto result = node.participants(buffer);

    REQUIRE(result.count == 3);
    REQUIRE_FALSE(result.truncated);
    const std::span<const plexus::graph::participant_record> filled{buffer.data(), result.count};

    // The reported origin is shown AS relayed: reported-by and via-whom both visible.
    const auto *reported = record_for(filled, id_origin);
    REQUIRE(reported != nullptr);
    REQUIRE(reported->origin.how == plexus::graph::observation::reported);
    REQUIRE(reported->origin.reporter == id_relay);
    REQUIRE(reported->reach.via == id_relay);

    // The genuinely direct peer's record in the SAME snapshot keeps the direct-only shape — the two
    // shapes coexist, never blended.
    const auto *direct = record_for(filled, id_direct);
    REQUIRE(direct != nullptr);
    REQUIRE(direct->origin.how == plexus::graph::observation::directly_observed);
    REQUIRE_FALSE(direct->origin.reporter.has_value());
    REQUIRE_FALSE(direct->reach.via.has_value());
    REQUIRE(direct->reach.transport == ep_direct);

    // The relay itself is a direct participant, distinct from the origin it relays.
    const auto *relay = record_for(filled, id_relay);
    REQUIRE(relay != nullptr);
    REQUIRE(relay->origin.how == plexus::graph::observation::directly_observed);
    REQUIRE_FALSE(relay->reach.via.has_value());
}

TEST_CASE("topics folds a reported origin's topics-with-types under the origin's node_id", "[graph]")
{
    host        h;
    inproc_node node{h.ex, h.disc, make_id(0x01), h.transport, make_opts()};

    const auto id_relay  = make_id(0x0A);
    const auto id_origin = make_id(0x0C);
    node.router().note_peer(id_relay, make_ep("host-a:5000"));
    node.router().ingest_peer_report(id_relay, reported_origin(id_origin, "sensor/temp", "Temp"));

    std::array<plexus::graph::topic_record, 8> buffer{};
    const auto result = node.topics(buffer);
    const std::span<const plexus::graph::topic_record> filled{buffer.data(), result.count};

    const auto *topic = topic_for(filled, id_origin);
    REQUIRE(topic != nullptr);
    REQUIRE(topic->name == "sensor/temp");
    REQUIRE(topic->role == plexus::graph::topic_role::publisher);
    REQUIRE(topic->types.count == 1);
    REQUIRE(topic->types.names[0] == "Temp");
}
