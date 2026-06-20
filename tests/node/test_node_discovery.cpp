#include "test_node_discovery_common.h"

using namespace node_discovery_fixture;

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
