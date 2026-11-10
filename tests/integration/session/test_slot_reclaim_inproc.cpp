#include "recording_observer.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace routing_inproc_fixture;

namespace {

struct edge_count final : plexus::io::observer
{
    edge_count()
            : edges(0)
    {
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    void on_peer_connected(const plexus::node_id &, std::string_view, plexus::io::peer_kind) override
    {
        ++edges;
    }

    void on_peer_disconnected(const plexus::node_id &, std::string_view, plexus::io::peer_kind) override
    {
        ++edges;
    }

    void on_peer_rejected(const plexus::node_id &, std::string_view, plexus::io::handshake_outcome) override
    {
        ++edges;
    }

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &) override
    {
        ++edges;
    }

    void on_participant(const plexus::io::participant_event &) override
    {
        ++edges;
    }

    void on_endpoint(std::string_view, const plexus::io::endpoint_event &) override
    {
        ++edges;
    }

    std::int32_t edges;
};

handshake_fsm_config older_cfg(std::uint8_t id_seed)
{
    handshake_fsm_config cfg     = make_cfg(id_seed);
    cfg.version_major            = 0;
    cfg.compatible_version_major = 0;
    return cfg;
}

}

TEST_CASE("slot reclaim: an accepted session's slot is erased once its dialer drops", "[integration][slot_reclaim]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.b.add_observer(rec);
    const std::size_t slots    = net.b.registry().slot_count();
    const std::size_t retiring = net.transport_b.retiring_size();

    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.b.registry().slot_count() == slots + 1);
    REQUIRE(rec.only_peer().connected == 1);

    net.a.hang_up(net.ep_b);
    net.drive();
    CHECK(net.b.registry().slot_count() == slots);
    CHECK(net.transport_b.retiring_size() == retiring);
    CHECK(rec.only_peer().disconnected == 1);
    net.b.remove_observer(rec);
}

TEST_CASE("slot reclaim: an accepted session refused before its handshake completes leaves no slot behind", "[integration][slot_reclaim]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.b.add_observer(rec);
    const std::size_t slots = net.b.registry().slot_count();
    transport_t transport_c{net.ex, net.bus};
    engine c(transport_c, net.ex, older_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, net.sink, false);

    c.dial(net.ep_b);
    net.drive();
    REQUIRE(rec.only_peer().rejected == 1);
    CHECK(net.b.registry().slot_count() == slots);
    CHECK(rec.only_peer().connected == 0);
    CHECK(rec.only_peer().disconnected == 0);
    net.b.remove_observer(rec);
    c.hang_up(net.ep_b);
    net.drive();
}

TEST_CASE("slot reclaim: a dialed channel no slot claims is erased with no edge of any kind", "[integration][slot_reclaim]")
{
    manual_clock::reset();
    two_node net;
    edge_count seen;
    net.a.add_observer(seen);
    const std::size_t slots_a = net.a.registry().slot_count();
    const std::size_t slots_b = net.b.registry().slot_count();

    net.transport_a.dial(net.ep_b);
    net.drive();
    CHECK(net.a.registry().slot_count() == slots_a);
    CHECK(net.b.registry().slot_count() == slots_b);
    CHECK(seen.edges == 0);
    net.a.remove_observer(seen);
}

TEST_CASE("slot reclaim: closing the engine while an accepted session is live drains it without an erase racing the close", "[integration][slot_reclaim]")
{
    manual_clock::reset();
    two_node net;
    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.b.registry().slot_count() == 1);

    SECTION("with the session live")
    {
        net.b.close();
        net.drive();
        CHECK(net.b.pending().settled());
    }
    SECTION("with its dialer's hang-up in flight")
    {
        net.a.hang_up(net.ep_b);
        net.b.close();
        net.drive();
        CHECK(net.b.pending().settled());
    }
}
