// The byte-identity alloc gate on the evolved awareness path: re-noting an already-known DIRECT peer
// and looking it up adds zero allocation on the existing direct-only path (a first admission of a new
// identity on the heap twin is the only sanctioned alloc, warmed away before the measured window), and
// the fixed twin allocates nothing at all. This TU owns the single replaceable operator new/delete
// (support/alloc_counter.h), so it is its own executable.

#include "support/alloc_counter.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/fixed_peer_storage.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

namespace
{

using plexus::node_id;
using plexus::io::basic_known_peers;
using plexus::io::endpoint;
using plexus::io::fixed_peer_storage;
using plexus::io::known_peers;

node_id peer_id()
{
    node_id id{};
    id[0] = std::byte{0x01};
    return id;
}

endpoint short_ep()
{
    return endpoint{"udp", "a:1"}; // both fields fit the small-string buffer, so a copy never heap-allocates
}

}

TEST_CASE("note_peer of a known direct peer and lookup add zero allocation on the heap twin", "[integration][route]")
{
    known_peers table;
    const node_id peer = peer_id();
    const endpoint ep  = short_ep();

    table.note_peer(peer, ep, 1); // first admission: the sole sanctioned map-node alloc, warmed away

    constexpr int K = 1000;
    int hits        = 0;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
    {
        table.note_peer(peer, ep, static_cast<std::uint64_t>(i));
        if(table.lookup(peer).has_value())
            ++hits;
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(hits == K);
    REQUIRE(after - before == 0);
}

TEST_CASE("the fixed twin note_peer + lookup path allocates nothing at all", "[integration][route]")
{
    basic_known_peers<fixed_peer_storage<8, 4>> table;
    const node_id peer = peer_id();
    const endpoint ep  = short_ep();

    constexpr int K = 1000;
    int hits        = 0;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    table.note_peer(peer, ep, 1); // even the first admission is inline: no map node, no heap
    for(int i = 0; i < K; ++i)
    {
        table.note_peer(peer, ep, static_cast<std::uint64_t>(i));
        if(table.lookup(peer).has_value())
            ++hits;
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(hits == K);
    REQUIRE(after - before == 0);
}
