#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/default_discovery.h"

#include "plexus/discovery/universe.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/graph/participant_record.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

// Enumeration is universe-scoped BY CONSTRUCTION. participants() takes no universe argument
// and applies no query-time filter; a foreign-universe announcement is dropped at
// multicast_discovery::on_inbound (ann->universe != m_options.universe) BEFORE note_peer, so it never
// enters the awareness table the snapshot sweeps. A same-universe control pair proves the boundary is
// the universe compare, not a dead socket: same-host wildcard multicast delivers every group's
// datagrams to every co-host socket, so the exclusion is the in-band compare against a live positive
// control, never an absence of traffic.

namespace {

namespace pasio = plexus::asio;

using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;
using plexus::discovery::discovery_options;
using plexus::discovery::universe_from_label;
using plexus::discovery::universe_scoping;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0x5a)};
    return id;
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

discovery_options universe_opts(std::string_view label, std::string group)
{
    discovery_options opts;
    opts.universe = universe_from_label(label);
    opts.scoping  = universe_scoping::soft;
    opts.group    = std::move(group);
    return opts;
}

template<typename Pred>
bool pump_until(::asio::io_context &io, Pred pred, std::chrono::milliseconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
    return pred();
}

bool snapshot_sees(asio_node &node, const plexus::node_id &id)
{
    std::array<plexus::graph::participant_record, 16> buffer{};
    const auto result = node.participants(buffer);
    return std::any_of(buffer.begin(), buffer.begin() + result.count, [&](const auto &rec) { return rec.id == id; });
}

}

TEST_CASE("two_universe: each node's participants() snapshot excludes a foreign-universe peer while "
          "a same-universe control sees its peer",
          "[graph]")
{
    const auto k_bound            = std::chrono::seconds(20);
    const auto k_settle           = std::chrono::milliseconds(2500);
    const std::string base        = "239.255.0.7";
    const std::uint16_t base_port = static_cast<std::uint16_t>(24000 + (plexus::testing::process_id() % 500) * 9);

    ::asio::io_context io;
    pasio::default_discovery da1{io, universe_opts("alpha", base)};
    pasio::default_discovery da2{io, universe_opts("alpha", base)};
    pasio::default_discovery db{io, universe_opts("beta", base)};
    pasio::asio_transport ta1{io}, ta2{io}, tb{io};

    const auto id_a1 = make_id(0xA1);
    const auto id_a2 = make_id(0xA2);
    const auto id_b  = make_id(0xB3);
    asio_node a1{io, da1.discovery(), id_a1, ta1, make_opts()};
    asio_node a2{io, da2.discovery(), id_a2, ta2, make_opts()};
    asio_node b{io, db.discovery(), id_b, tb, make_opts()};

    a1.listen({"tcp", "127.0.0.1:" + std::to_string(base_port)});
    a2.listen({"tcp", "127.0.0.1:" + std::to_string(base_port + 1)});
    b.listen({"tcp", "127.0.0.1:" + std::to_string(base_port + 2)});

    // The same-universe control is the positive gate: if it never rendezvous the host has no usable
    // multicast loopback, so the exclusion below would be indistinguishable from a dead socket.
    const bool control = pump_until(io, [&] { return snapshot_sees(a1, id_a2) && snapshot_sees(a2, id_a1); }, k_bound);
    if(!control)
        SKIP("multicast loopback unavailable on this host: the same-universe control reached no snapshot awareness within the bound");

    // Give the beta node several announce periods on the shared base group; its datagrams ARE
    // delivered on-host and only the inbound universe compare keeps it out of the alpha tables.
    pump_until(io, [] { return false; }, k_settle);

    REQUIRE(snapshot_sees(a1, id_a2));
    REQUIRE(snapshot_sees(a2, id_a1));
    REQUIRE_FALSE(snapshot_sees(a1, id_b));
    REQUIRE_FALSE(snapshot_sees(a2, id_b));
    REQUIRE_FALSE(snapshot_sees(b, id_a1));
    REQUIRE_FALSE(snapshot_sees(b, id_a2));
}
