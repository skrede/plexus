#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/io/node_name.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <string>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <string_view>

using namespace std::chrono_literals;

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::service_info;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

// Cards and goodbyes reach the node only when a case sends them.
class scripted_discovery final : public plexus::discovery::discovery
{
public:
    void advertise(const service_info &) override
    {
    }

    void browse(const resolved_callback &on_resolved) override
    {
        m_resolved = on_resolved;
    }

    void stop() override
    {
    }

    void on_withdrawn(const withdrawn_callback &on_withdrawn) override
    {
        m_withdrawn = on_withdrawn;
    }

    void announce(const service_info &card)
    {
        m_resolved(card);
    }

    void goodbye(const service_info &card)
    {
        m_withdrawn(card);
    }

private:
    resolved_callback m_resolved;
    withdrawn_callback m_withdrawn;
};

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Nothing listens on the card's endpoint, so the peer is known by card alone.
service_info card_of(const plexus::node_id &id)
{
    return service_info{"peer", plexus::io::endpoint{"", "host-b"}, {{"node_id", plexus::discovery::detail::hex_encode(id)}, {"plexus/inproc/port", "6000"}}};
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect                = plexus::io::reconnect_config{50ms, 2000ms, std::nullopt, std::nullopt};
    opts.redial_seed              = 0xC0FFEEu;
    opts.liveliness.awareness_ttl = 300ms;
    return opts;
}

template<typename Pred>
bool run_until(inproc_executor<> &ex, Pred pred, std::chrono::milliseconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    for(ex.drain(); !pred() && std::chrono::steady_clock::now() < deadline; ex.drain())
        std::this_thread::sleep_for(5ms);
    return pred();
}

bool remembers(inproc_node &n, const plexus::node_id &peer, std::string_view fqn)
{
    return std::ranges::any_of(n.router().messages().remembered_topics(plexus::io::node_name_of(peer)), [&](const auto &d) { return d.fqn == fqn; });
}

struct host
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    scripted_discovery da;
    scripted_discovery db;
    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};
    inproc_node a{ex, da, id_a, ta, make_opts()};

    bool a_sees_b() const
    {
        return a.router().is_connected(id_b);
    }
};

}

TEST_CASE("known peers: a peer whose awareness aged out gets no demand from a later subscriber", "[node][known_peers]")
{
    host h;
    h.da.announce(card_of(h.id_b));
    REQUIRE(h.a.router().known().contains(h.id_b));
    REQUIRE(run_until(h.ex, [&] { return !h.a.router().known().contains(h.id_b); }, 5s));

    plexus::subscriber<> s{h.a, "pruning/aged", [](std::span<const std::byte>) {}};
    h.ex.drain();
    REQUIRE_FALSE(h.a_sees_b());
    REQUIRE_FALSE(remembers(h.a, h.id_b, "pruning/aged"));
}

TEST_CASE("known peers: a goodbye removes the peer from the demand targets", "[node][known_peers]")
{
    host h;
    h.da.announce(card_of(h.id_b));
    h.da.goodbye(card_of(h.id_b));
    REQUIRE_FALSE(h.a.router().known().contains(h.id_b));

    plexus::subscriber<> s{h.a, "pruning/goodbye", [](std::span<const std::byte>) {}};
    h.ex.drain();
    REQUIRE_FALSE(remembers(h.a, h.id_b, "pruning/goodbye"));
}

TEST_CASE("known peers: a peer that dialed in with no card still receives a later subscriber's demand", "[node][known_peers]")
{
    host h;
    inproc_node b{h.ex, h.db, h.id_b, h.tb, make_opts()};
    h.a.listen({"inproc", "host-a:5000"});
    plexus::publisher<> p{b, "pruning/dialed"};
    b.dial({"inproc", "host-a:5000"});
    REQUIRE(run_until(h.ex, [&] { return h.a_sees_b(); }, 2s));
    REQUIRE_FALSE(h.a.router().known().contains(h.id_b));

    std::size_t delivered = 0;
    plexus::subscriber<> s{h.a, "pruning/dialed", [&](std::span<const std::byte>) { ++delivered; }};
    h.ex.drain();
    REQUIRE(remembers(h.a, h.id_b, "pruning/dialed"));

    const std::array<std::byte, 1> payload{std::byte{0x2A}};
    p.publish(payload);
    h.ex.drain();
    REQUIRE(delivered == 1);
}

TEST_CASE("known peers: retiring a subscriber after a dialed-in peer left leaves no demand to replay", "[node][known_peers]")
{
    host h;
    std::optional<plexus::subscriber<>> s;
    {
        inproc_node b{h.ex, h.db, h.id_b, h.tb, make_opts()};
        h.a.listen({"inproc", "host-a:5000"});
        b.dial({"inproc", "host-a:5000"});
        REQUIRE(run_until(h.ex, [&] { return h.a_sees_b(); }, 2s));
        s.emplace(h.a, "pruning/left", [](std::span<const std::byte>) {});
        h.ex.drain();
        REQUIRE(remembers(h.a, h.id_b, "pruning/left"));
        b.close();
        REQUIRE(run_until(h.ex, [&] { return !h.a_sees_b(); }, 2s));
    }

    s.reset();
    h.ex.drain();
    REQUIRE_FALSE(remembers(h.a, h.id_b, "pruning/left"));
}
