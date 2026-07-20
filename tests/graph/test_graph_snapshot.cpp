#include "test_graph_node_common.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <algorithm>

using namespace graph_node_fixture;

namespace {

bool holds_id(std::span<const plexus::graph::participant_record> filled, const plexus::node_id &id)
{
    return std::any_of(filled.begin(), filled.end(), [&](const auto &rec) { return rec.id == id; });
}

const plexus::graph::participant_record *record_for(std::span<const plexus::graph::participant_record> filled, const plexus::node_id &id)
{
    const auto it = std::find_if(filled.begin(), filled.end(), [&](const auto &rec) { return rec.id == id; });
    return it == filled.end() ? nullptr : &*it;
}

}

TEST_CASE("participants enumerates known peers keyed by node_id as direct-only records", "[graph]")
{
    host       h;
    inproc_node node{h.ex, h.disc, make_id(0x01), h.transport, make_opts()};

    const auto id_b = make_id(0x0B);
    const auto id_c = make_id(0x0C);
    const auto ep_b = make_ep("host-b:6000");
    const auto ep_c = make_ep("host-c:7000");
    node.router().note_peer(id_b, ep_b);
    node.router().note_peer(id_c, ep_c);

    std::array<plexus::graph::participant_record, 8> buffer{};
    const auto result = node.participants(buffer);

    REQUIRE(result.count == 2);
    REQUIRE_FALSE(result.truncated);

    const std::span<const plexus::graph::participant_record> filled{buffer.data(), result.count};
    REQUIRE(holds_id(filled, id_b));
    REQUIRE(holds_id(filled, id_c));

    for(const auto &rec : filled)
    {
        REQUIRE(rec.origin.how == plexus::graph::observation::directly_observed);
        REQUIRE_FALSE(rec.origin.reporter.has_value());
        REQUIRE_FALSE(rec.reach.via.has_value());
    }
    REQUIRE(record_for(filled, id_b)->reach.transport == ep_b);
    REQUIRE(record_for(filled, id_c)->reach.transport == ep_c);
}
