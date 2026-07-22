#include "plexus/io/endpoint.h"
#include "plexus/io/route_select.h"
#include "plexus/io/route_candidate.h"

#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/wire/udp_dedup_window.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace
{

using plexus::node_id;
using plexus::graph::observation;
using plexus::graph::provenance;
using plexus::graph::route;
using plexus::io::endpoint;
using plexus::io::route_candidate;
using plexus::io::route_select;
using plexus::io::route_select_npos;
using plexus::io::route_usage;

node_id id_with(std::uint8_t tag)
{
    node_id id{};
    id[0] = std::byte{tag};
    return id;
}

route_candidate relayed(std::uint8_t hop, std::uint64_t tag)
{
    return route_candidate{
        route{endpoint{"udp", "203.0.113.9:9000"}, id_with(0xFE)},
        provenance{observation::reported, id_with(0xFE)},
        hop,
        plexus::wire::udp_dedup_window{},
        tag};
}

route_candidate direct(std::uint64_t tag)
{
    return route_candidate{
        route{endpoint{"udp", "203.0.113.7:9000"}, std::nullopt},
        provenance{observation::directly_observed, std::nullopt},
        0,
        plexus::wire::udp_dedup_window{},
        tag};
}

using plexus::graph::reachability;

route_candidate relayed_via(std::uint8_t via_tag, std::uint8_t hop, std::uint64_t tag, reachability reach_status)
{
    return route_candidate{
        route{endpoint{"udp", "203.0.113.9:9000"}, id_with(via_tag)},
        provenance{observation::reported, id_with(via_tag), reach_status},
        hop,
        plexus::wire::udp_dedup_window{},
        tag};
}

}

TEST_CASE("route_select degenerates a one-direct span to that direct candidate", "[graph][route_select]")
{
    const std::array<route_candidate, 1> span{direct(100)};
    const auto pick = route_select(span);
    REQUIRE(pick == 0);
    REQUIRE(span[pick].is_direct());
    REQUIRE(span[pick].last_refreshed == 100);
}

TEST_CASE("route_select degenerates a one-relayed span to that relayed candidate", "[graph][route_select]")
{
    const std::array<route_candidate, 1> span{relayed(4, 40)};
    const auto pick = route_select(span);
    REQUIRE(pick == 0);
    REQUIRE_FALSE(span[pick].is_direct());
}

TEST_CASE("route_select prefers the direct candidate placed before a relayed one", "[graph][route_select]")
{
    const std::array<route_candidate, 2> span{direct(100), relayed(1, 10)};
    const auto pick = route_select(span);
    REQUIRE(span[pick].is_direct());
    REQUIRE(span[pick].last_refreshed == 100);
}

TEST_CASE("route_select prefers the direct candidate placed after a relayed one", "[graph][route_select]")
{
    const std::array<route_candidate, 2> span{relayed(1, 10), direct(100)};
    const auto pick = route_select(span);
    REQUIRE(span[pick].is_direct());
    REQUIRE(span[pick].last_refreshed == 100);
}

TEST_CASE("route_select ranks a live relayed row over a same-hop unreachable one for the same origin", "[graph][route_select]")
{
    const std::array<route_candidate, 2> dead_first{relayed_via(0xA1, 1, 91, reachability::unreachable), relayed_via(0xA2, 1, 92, reachability::reachable)};
    const auto pick_a = route_select(dead_first);
    REQUIRE(pick_a == 1);
    REQUIRE(dead_first[pick_a].last_refreshed == 92);

    const std::array<route_candidate, 2> live_first{relayed_via(0xA2, 1, 92, reachability::reachable), relayed_via(0xA1, 1, 91, reachability::unreachable)};
    const auto pick_b = route_select(live_first);
    REQUIRE(pick_b == 0);
    REQUIRE(live_first[pick_b].last_refreshed == 92);
}

TEST_CASE("route_select still picks an origin's only relayed row once it degrades to unreachable", "[graph][route_select]")
{
    const std::array<route_candidate, 1> span{relayed_via(0xA1, 1, 91, reachability::unreachable)};
    const auto pick = route_select(span);
    REQUIRE(pick == 0);
    REQUIRE_FALSE(span[pick].is_direct());
}

TEST_CASE("route_select picks the fewest-hop relayed candidate, first-in-span breaking ties", "[graph][route_select]")
{
    const std::array<route_candidate, 4> span{relayed(2, 20), relayed(1, 11), relayed(1, 12), relayed(3, 30)};
    const auto pick = route_select(span);
    REQUIRE(pick == 1);
    REQUIRE(span[pick].last_refreshed == 11);
}

TEST_CASE("route_select is order-independent for a fixed candidate set with a unique winner", "[graph][route_select]")
{
    std::vector<route_candidate> base;
    base.push_back(relayed(3, 3));
    base.push_back(direct(100));
    base.push_back(relayed(1, 1));
    base.push_back(relayed(2, 2));

    std::array<std::size_t, 4> order{0, 1, 2, 3};
    std::sort(order.begin(), order.end());
    do
    {
        std::vector<route_candidate> shuffled;
        shuffled.reserve(order.size());
        for(const std::size_t i : order)
            shuffled.push_back(base[i]);

        const auto pick = route_select(shuffled);
        REQUIRE(pick != route_select_npos);
        REQUIRE(shuffled[pick].is_direct());
        REQUIRE(shuffled[pick].last_refreshed == 100);
    } while(std::next_permutation(order.begin(), order.end()));
}

TEST_CASE("route_select returns npos for an empty span without dereferencing", "[graph][route_select]")
{
    const std::span<const route_candidate> empty{};
    REQUIRE(route_select(empty) == route_select_npos);
}

TEST_CASE("route_select under never rejects a relayed-only span, yielding npos", "[graph][route_select][usage]")
{
    const std::array<route_candidate, 2> span{relayed(1, 10), relayed(2, 20)};
    REQUIRE(route_select(span, route_usage::never) == route_select_npos);
    REQUIRE(route_select(span, route_usage::prefer_direct) == 0);
    REQUIRE(route_select(span, route_usage::allow_relayed) == 0);
}

TEST_CASE("route_select keeps a direct winner across every usage mode", "[graph][route_select][usage]")
{
    const std::array<route_candidate, 3> span{relayed(1, 10), direct(100), relayed(2, 20)};
    for(const route_usage usage : {route_usage::never, route_usage::prefer_direct, route_usage::allow_relayed})
    {
        const auto pick = route_select(span, usage);
        REQUIRE(pick != route_select_npos);
        REQUIRE(span[pick].is_direct());
        REQUIRE(span[pick].last_refreshed == 100);
    }
}

TEST_CASE("route_select prefer_direct and allow_relayed keep a relayed pick that never rejects", "[graph][route_select][usage]")
{
    const std::array<route_candidate, 1> span{relayed(3, 33)};
    REQUIRE(route_select(span, route_usage::prefer_direct) == 0);
    REQUIRE(route_select(span, route_usage::allow_relayed) == 0);
    REQUIRE(route_select(span, route_usage::never) == route_select_npos);
}

TEST_CASE("route_select usage overload leaves an empty span at npos for every mode", "[graph][route_select][usage]")
{
    const std::span<const route_candidate> empty{};
    REQUIRE(route_select(empty, route_usage::never) == route_select_npos);
    REQUIRE(route_select(empty, route_usage::prefer_direct) == route_select_npos);
    REQUIRE(route_select(empty, route_usage::allow_relayed) == route_select_npos);
}
