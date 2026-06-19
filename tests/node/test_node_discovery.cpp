// The node discovery-wiring oracle, over the live static_discovery:
//   (a) advertise-at-birth — a constructed node's card is visible to a browser, and a
//       dial-only (never-listening) node is visible too (the transparency invariant);
//   (b) listen() appends exactly one plexus/<scheme>/port key with the bound port and
//       re-advertises live to an already-registered browser;
//   (c) browse-to-awareness — a peer's advertised card lands in the first node's
//       router().known() with the card-derived endpoint; the node skips its OWN card;
//   (d) the malformed-card reject table — bad hex / missing id / no port key produce no
//       awareness entry and no crash (untrusted multicast input).

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::service_info;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                        std::chrono::milliseconds(2000), std::nullopt,
                                        std::nullopt};
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect   = forever_cfg();
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

struct host
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> transport{ex, bus};
    static_discovery   disc{{}};
};

// A browser that records every card it is notified of, for asserting advertise-at-birth
// and the listen() live update.
struct recording_browser
{
    std::vector<service_info> seen;

    void attach(static_discovery &disc)
    {
        disc.browse([this](const service_info &s) { seen.push_back(s); });
    }

    const service_info *latest_for(const std::string &name) const
    {
        const service_info *found = nullptr;
        for(const auto &s : seen)
            if(s.name == name)
                found = &s;
        return found;
    }
};

bool has_key(const service_info &s, std::string_view key)
{
    return std::any_of(s.metadata.begin(), s.metadata.end(),
                       [&](const auto &kv) { return kv.first == key; });
}

bool has_port_key(const service_info &s)
{
    return std::any_of(s.metadata.begin(), s.metadata.end(),
                       [](const auto &kv)
                       {
                           return kv.first.rfind("plexus/", 0) == 0 && kv.first.size() > 5 &&
                                   kv.first.substr(kv.first.size() - 5) == "/port";
                       });
}

}

TEST_CASE("node discovery: a dial-only node advertises its card at birth", "[node][discovery]")
{
    host              h;
    recording_browser browser;
    browser.attach(h.disc);

    const auto  id = make_id(0xA1);
    inproc_node n{h.ex, h.disc, id, h.transport, make_opts()};

    const auto *card = browser.latest_for(plexus::io::node_name_of(id));
    REQUIRE(card != nullptr);
    // node_id key matches hex_encode(id); the schema key is present; NO port key yet
    // (a dial-only, never-listening node is still discoverable — the transparency
    // invariant).
    REQUIRE(has_key(*card, plexus::discovery::k_card_node_id_key));
    REQUIRE(has_key(*card, plexus::discovery::k_card_schema_key));
    REQUIRE(plexus::discovery::detail::hex_decode(card->metadata.front().second) == id);
    REQUIRE_FALSE(has_port_key(*card));
}

TEST_CASE("node discovery: listen() appends a port key and re-advertises live", "[node][discovery]")
{
    host              h;
    recording_browser browser;
    browser.attach(h.disc);

    const auto  id = make_id(0xB2);
    inproc_node n{h.ex, h.disc, id, h.transport, make_opts()};
    const auto  name = plexus::io::node_name_of(id);

    REQUIRE_FALSE(has_port_key(*browser.latest_for(name)));

    n.listen({"inproc", "host-b:7000"});

    const auto *card = browser.latest_for(name);
    REQUIRE(card != nullptr);
    REQUIRE(has_port_key(*card));
    // Exactly one plexus/<scheme>/port key, carrying the bound explicit port.
    const auto count =
            std::count_if(card->metadata.begin(), card->metadata.end(),
                          [](const auto &kv) { return kv.first == "plexus/inproc/port"; });
    REQUIRE(count == 1);
    REQUIRE(plexus::discovery::read_transport_port(card->metadata, "inproc") ==
            std::uint16_t{7000});
}

TEST_CASE("node discovery: a peer's card lands in known(), own card skipped", "[node][discovery]")
{
    host        h;
    const auto  id_a = make_id(0x0A);
    inproc_node a{h.ex, h.disc, id_a, h.transport, make_opts()};

    // A's own listen re-advertise must NOT make A aware of itself.
    a.listen({"inproc", "host-a:5000"});
    REQUIRE_FALSE(a.router().known().contains(id_a));

    // A second node on the SAME discovery advertises at construction + listen; A's
    // retained browse handler notes it with the card-derived endpoint.
    inproc_transport<> transport_b2{h.ex, h.bus};
    const auto         id_b = make_id(0x0B);
    inproc_node        b{h.ex, h.disc, id_b, transport_b2, make_opts()};
    b.listen({"inproc", "host-b:6000"});

    REQUIRE(a.router().known().contains(id_b));
    const auto ep = a.router().known().lookup(id_b);
    REQUIRE(ep.has_value());
    REQUIRE(ep->scheme == "inproc");
    REQUIRE(ep->address == "host-b:6000");
    // B in turn became aware of A (A had already listened before B browsed).
    REQUIRE(b.router().known().contains(id_a));
}

TEST_CASE("node discovery: malformed cards produce no awareness entry and no crash",
          "[node][discovery]")
{
    host        h;
    const auto  id_a = make_id(0xAA);
    inproc_node a{h.ex, h.disc, id_a, h.transport, make_opts()};
    a.listen({"inproc", "host-a:5000"});

    const auto card_with = [](std::vector<std::pair<std::string, std::string>> md)
    { return service_info{"peer", plexus::io::endpoint{"", "host-x"}, std::move(md)}; };

    // 31 hex chars (too short).
    h.disc.advertise(
            card_with({{"node_id", std::string(31, 'a')}, {"plexus/inproc/port", "9000"}}));
    // Uppercase hex (hex_encode emits lowercase; uppercase is rejected).
    h.disc.advertise(
            card_with({{"node_id", std::string(32, 'A')}, {"plexus/inproc/port", "9000"}}));
    // Non-hex garbage.
    h.disc.advertise(
            card_with({{"node_id", std::string(32, 'z')}, {"plexus/inproc/port", "9000"}}));
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
    h.disc.advertise(card_with({{"node_id", plexus::discovery::detail::hex_encode(id_ok)},
                                {"plexus/inproc/port", "8100"}}));
    REQUIRE(a.router().known().contains(id_ok));
    REQUIRE(a.router().known().lookup(id_ok)->address == "host-x:8100");
}
