#include "test_node_discovery_common.h"

#include "plexus/discovery/discovery.h"

#include "plexus/log/logger.h"

#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>
#include <string_view>

using namespace node_discovery_fixture;

namespace {

constexpr std::string_view k_card_not_advertised = "plexus: discovery card_not_advertised";

class refusing_discovery final : public plexus::discovery::discovery
{
public:
    refusing_discovery()
            : m_advertised(0)
    {
    }

    bool advertise(const service_info &) override
    {
        ++m_advertised;
        return false;
    }

    void browse(const resolved_callback &) override
    {
    }

    void stop() override
    {
    }

    std::int32_t advertised() const
    {
        return m_advertised;
    }

private:
    std::int32_t m_advertised;
};

class capturing_logger final : public plexus::log::logger
{
public:
    void warn(std::string_view message) override
    {
        messages.emplace_back(message);
    }

    std::vector<std::string> messages;
};

std::int64_t refusals_in(const capturing_logger &log)
{
    return std::ranges::count(log.messages, k_card_not_advertised);
}

}

TEST_CASE("node discovery: a dial-only node advertises its card at birth", "[node][discovery]")
{
    host h;
    recording_browser browser;
    browser.attach(h.disc);

    const auto id = make_id(0xA1);
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
    host h;
    recording_browser browser;
    browser.attach(h.disc);

    const auto id = make_id(0xB2);
    inproc_node n{h.ex, h.disc, id, h.transport, make_opts()};
    const auto name = plexus::io::node_name_of(id);

    REQUIRE_FALSE(has_port_key(*browser.latest_for(name)));

    n.listen({"inproc", "host-b:7000"});

    const auto *card = browser.latest_for(name);
    REQUIRE(card != nullptr);
    REQUIRE(has_port_key(*card));
    // Exactly one plexus/<scheme>/port key, carrying the bound explicit port.
    const auto count = std::count_if(card->metadata.begin(), card->metadata.end(), [](const auto &kv) { return kv.first == "plexus/inproc/port"; });
    REQUIRE(count == 1);
    REQUIRE(plexus::discovery::read_transport_port(card->metadata, "inproc") == std::uint16_t{7000});
}

TEST_CASE("node discovery: a card the discovery backend refuses is reported through the node's logger", "[node][discovery]")
{
    host refused_host;
    refusing_discovery refusing;
    capturing_logger refused_log;
    auto refused_opts   = make_opts();
    refused_opts.logger = &refused_log;
    inproc_node refused{refused_host.ex, refusing, make_id(0xC3), refused_host.transport, refused_opts};

    REQUIRE(refusing.advertised() == 1);
    REQUIRE(refusals_in(refused_log) == 1);
    refused.listen({"inproc", "host-c:7000"});
    REQUIRE(refusing.advertised() == 2);
    REQUIRE(refusals_in(refused_log) == 2);

    host accepted_host;
    capturing_logger accepted_log;
    auto accepted_opts   = make_opts();
    accepted_opts.logger = &accepted_log;
    inproc_node accepted{accepted_host.ex, accepted_host.disc, make_id(0xC4), accepted_host.transport, accepted_opts};
    accepted.listen({"inproc", "host-d:7000"});

    REQUIRE(refusals_in(accepted_log) == 0);
}
