#include "test_graph_node_common.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>

using namespace graph_node_fixture;

namespace {

std::size_t known_count(const inproc_node &node)
{
    std::size_t seen = 0;
    node.router().known().for_each([&](const plexus::node_id &, const plexus::io::endpoint &) { ++seen; });
    return seen;
}

}

TEST_CASE("participants reject-and-counts at the span boundary without eviction or allocation", "[graph]")
{
    constexpr std::size_t k_capacity = 4;
    constexpr std::size_t k_peers    = k_capacity + 1;

    host       h;
    inproc_node node{h.ex, h.disc, make_id(0x01), h.transport, make_opts()};
    for(std::uint8_t i = 0; i < k_peers; ++i)
        node.router().note_peer(make_id(0x10 + i), make_ep("p:" + std::to_string(i)));

    std::array<plexus::graph::participant_record, k_capacity> buffer{};

    const std::size_t before = plexus::testing::alloc_count();
    const auto        result = node.participants(buffer);
    const std::size_t after  = plexus::testing::alloc_count();

    REQUIRE(result.count == k_capacity);
    REQUIRE(result.truncated);
    REQUIRE(after - before == 0);

    // Reject-and-count is a read-only capacity signal: no peer is evicted, so the awareness
    // table still holds every one of the N+1 known peers.
    REQUIRE(known_count(node) == k_peers);
}
