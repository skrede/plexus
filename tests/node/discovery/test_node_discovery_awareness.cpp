#include "test_node_discovery_common.h"

using namespace node_discovery_fixture;

TEST_CASE("node discovery: a peer's card lands in known(), own card skipped", "[node][discovery]")
{
    host h;
    const auto id_a = make_id(0x0A);
    inproc_node a{h.ex, h.disc, id_a, h.transport, make_opts()};

    // A's own listen re-advertise must NOT make A aware of itself.
    a.listen({"inproc", "host-a:5000"});
    REQUIRE_FALSE(a.router().known().contains(id_a));

    // A second node on the SAME discovery advertises at construction + listen; A's
    // retained browse handler notes it with the card-derived endpoint.
    inproc_transport<> transport_b2{h.ex, h.bus};
    const auto id_b = make_id(0x0B);
    inproc_node b{h.ex, h.disc, id_b, transport_b2, make_opts()};
    b.listen({"inproc", "host-b:6000"});

    REQUIRE(a.router().known().contains(id_b));
    const auto ep = a.router().known().lookup(id_b);
    REQUIRE(ep.has_value());
    REQUIRE(ep->scheme == "inproc");
    REQUIRE(ep->address == "host-b:6000");
    // B in turn became aware of A (A had already listened before B browsed).
    REQUIRE(b.router().known().contains(id_a));
}

TEST_CASE("node discovery: malformed cards produce no awareness entry and no crash", "[node][discovery]")
{
    host h;
    const auto id_a = make_id(0xAA);
    inproc_node a{h.ex, h.disc, id_a, h.transport, make_opts()};
    a.listen({"inproc", "host-a:5000"});

    const auto card_with = [](std::vector<std::pair<std::string, std::string>> md) { return service_info{"peer", plexus::io::endpoint{"", "host-x"}, std::move(md)}; };

    // 31 hex chars (too short).
    h.disc.advertise(card_with({{"node_id", std::string(31, 'a')}, {"plexus/inproc/port", "9000"}}));
    // Uppercase hex (hex_encode emits lowercase; uppercase is rejected).
    h.disc.advertise(card_with({{"node_id", std::string(32, 'A')}, {"plexus/inproc/port", "9000"}}));
    // Non-hex garbage.
    h.disc.advertise(card_with({{"node_id", std::string(32, 'z')}, {"plexus/inproc/port", "9000"}}));
    // Missing node_id key entirely.
    h.disc.advertise(card_with({{"plexus/inproc/port", "9000"}}));
    // A valid node_id but NO usable port key.
    const auto id_np = make_id(0x4D);
    h.disc.advertise(card_with({{"node_id", plexus::discovery::detail::hex_encode(id_np)}}));

    // None of the malformed adverts produced an entry; the no-port-key valid id is also
    // not noted (no dialable endpoint to record).
    REQUIRE_FALSE(a.router().known().contains(id_np));

    // A well-formed card AFTER the reject table still works (the handler did not wedge).
    const auto id_ok = make_id(0x5E);
    h.disc.advertise(card_with({{"node_id", plexus::discovery::detail::hex_encode(id_ok)}, {"plexus/inproc/port", "8100"}}));
    REQUIRE(a.router().known().contains(id_ok));
    REQUIRE(a.router().known().lookup(id_ok)->address == "host-x:8100");
}
