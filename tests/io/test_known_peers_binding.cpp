#include "plexus/io/endpoint.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/fixed_peer_storage.h"

#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

using plexus::io::basic_known_peers;
using plexus::io::endpoint;
using plexus::io::fixed_peer_storage;
using plexus::io::known_peers;
using plexus::io::route_candidate;
using plexus::io::route_options;
using plexus::node_id;
using plexus::graph::observation;
using plexus::graph::provenance;
using plexus::graph::route;

namespace {

node_id id_of(std::byte b)
{
    node_id id{};
    id[0] = b;
    return id;
}

endpoint ep_of(const char *addr)
{
    return endpoint{.scheme = "tcp", .address = addr};
}

route_candidate relayed_via(const node_id &via)
{
    route_candidate c{};
    c.reach  = route{endpoint{}, via};
    c.origin = provenance{observation::reported, via};
    c.hop    = 1;
    return c;
}

template<typename Table>
void exercise_mark(Table &table)
{
    const node_id a       = id_of(std::byte{0x01});
    const node_id relayed = id_of(std::byte{0x02});
    REQUIRE(table.note_peer(a, ep_of("a:1"), 5));
    table.invalidate_binding(a);
    CHECK(table.binding_invalid(a));
    CHECK_FALSE(table.dial_target(a));
    CHECK(table.lookup(a) == ep_of("a:1"));
    CHECK(table.contains(a));
    CHECK(table.candidates(a).size() == 1);
    table.refresh(a, 9);
    table.note_reported(a, relayed_via(relayed), 1, 9, route_options{});
    CHECK(table.binding_invalid(a));
    table.invalidate_binding(id_of(std::byte{0x03}));
    CHECK_FALSE(table.binding_invalid(id_of(std::byte{0x03})));
    table.note_reported(relayed, relayed_via(a), 1, 9, route_options{});
    table.invalidate_binding(relayed);
    CHECK_FALSE(table.binding_invalid(relayed));
}

template<typename Table>
void exercise_clear(Table &table)
{
    const node_id a = id_of(std::byte{0x01});
    REQUIRE(table.note_peer(a, ep_of("a:1"), 5));
    table.invalidate_binding(a);
    CHECK_FALSE(table.note_peer(a, ep_of("a:1"), 6));
    CHECK_FALSE(table.binding_invalid(a));
    CHECK(table.dial_target(a) == ep_of("a:1"));
    table.invalidate_binding(a);
    CHECK(table.note_peer(a, ep_of("a:2"), 7));
    CHECK(table.dial_target(a) == ep_of("a:2"));
    table.invalidate_binding(a);
    CHECK_FALSE(table.note_peer(a, ep_of("a:2")));
    CHECK(table.dial_target(a) == ep_of("a:2"));
}

template<typename Table>
void exercise_lifetime(Table &table)
{
    const node_id a = id_of(std::byte{0x01});
    const node_id b = id_of(std::byte{0x02});
    table.note_peer(a, ep_of("a:1"), 5);
    table.invalidate_binding(a);
    REQUIRE(table.forget(a));
    CHECK_FALSE(table.binding_invalid(a));
    table.note_peer(b, ep_of("b:1"), 5);
    CHECK_FALSE(table.binding_invalid(b));
    table.invalidate_binding(b);
    table.expire_older_than(6, [](const node_id &) {});
    CHECK_FALSE(table.contains(b));
    CHECK_FALSE(table.binding_invalid(b));
    table.note_reported(a, relayed_via(b), 1, 7, route_options{});
    CHECK_FALSE(table.binding_invalid(a));
}

template<typename Table>
void exercise_binding()
{
    Table marked;
    exercise_mark(marked);
    Table cleared;
    exercise_clear(cleared);
    Table aged;
    exercise_lifetime(aged);
}

}

TEST_CASE("known_peers_binding default storage keeps a flagged binding undialable until the next card", "[io][known_peers]")
{
    exercise_binding<known_peers>();
}

TEST_CASE("known_peers_binding fixed variant keeps a flagged binding undialable until the next card", "[io][known_peers]")
{
    exercise_binding<basic_known_peers<fixed_peer_storage<8>>>();
}

TEST_CASE("known_peers_binding a freed fixed slot hands no flag to the next identity", "[io][known_peers]")
{
    basic_known_peers<fixed_peer_storage<1>> table;
    exercise_lifetime(table);
}
