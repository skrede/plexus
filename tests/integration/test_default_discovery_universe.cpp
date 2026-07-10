#include "test_default_discovery_lan_common.h"

#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/universe.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <chrono>
#include <vector>
#include <cstdint>
#include <string_view>

using namespace default_discovery_lan_fixture;
using plexus::discovery::discovery_options;
using plexus::discovery::universe_from_label;
using plexus::discovery::universe_group;
using plexus::discovery::universe_scoping;

// HONESTY FRAME: hard scoping isolates across HOSTS at the wire (kernel/IGMP filters a
// foreign-universe group before delivery). On ONE host, wildcard-bound multicast still delivers
// every group's datagrams to every co-host socket, so the same-host guard is the in-band universe
// compare, not the group membership. The cross-universe case below therefore proves NON-awareness
// through the compare (the announcement IS delivered, then dropped) against a same-run positive
// control — a dead network cannot masquerade as isolation.

namespace {

discovery_options universe_opts(std::string_view label, universe_scoping scoping, std::string group)
{
    discovery_options opts;
    opts.universe = universe_from_label(label);
    opts.scoping  = scoping;
    opts.group    = std::move(group);
    return opts;
}

}

TEST_CASE("cross-universe nodes stay mutually unaware over real multicast while a same-run "
          "same-universe control rendezvous — the compare partitions, not a dead socket",
          "[integration][discovery][universe]")
{
    constexpr int k_runs          = 3;
    const auto k_bound            = std::chrono::seconds(20);
    const auto k_settle           = std::chrono::milliseconds(2500);
    const std::string base        = "239.255.0.7";
    const std::uint16_t base_port = static_cast<std::uint16_t>(21000 + (plexus::testing::process_id() % 500) * 9);

    std::vector<std::int64_t> control_ms;

    for(int run = 0; run < k_runs; ++run)
    {
        ::asio::io_context io;
        pasio::default_discovery da1{io, universe_opts("alpha", universe_scoping::soft, base)};
        pasio::default_discovery da2{io, universe_opts("alpha", universe_scoping::soft, base)};
        pasio::default_discovery db{io, universe_opts("beta", universe_scoping::soft, base)};
        pasio::asio_transport ta1{io}, ta2{io}, tb{io};

        const auto id_a1 = make_id(0xA1);
        const auto id_a2 = make_id(0xA2);
        const auto id_b  = make_id(0xB3);
        asio_node a1{io, da1.discovery(), id_a1, ta1, make_opts()};
        asio_node a2{io, da2.discovery(), id_a2, ta2, make_opts()};
        asio_node b{io, db.discovery(), id_b, tb, make_opts()};

        const std::uint16_t port = static_cast<std::uint16_t>(base_port + 3 * run);
        a1.listen({"tcp", "127.0.0.1:" + std::to_string(port)});
        a2.listen({"tcp", "127.0.0.1:" + std::to_string(port + 1)});
        b.listen({"tcp", "127.0.0.1:" + std::to_string(port + 2)});

        const auto t0      = std::chrono::steady_clock::now();
        const bool control = pump_until(io, [&] { return a1.router().known().contains(id_a2) && a2.router().known().contains(id_a1); }, k_bound);
        const auto ctrl_dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

        if(!control && run == 0)
            SKIP("multicast loopback unavailable on this host: the same-universe positive control reached no awareness within the bound");
        REQUIRE(control);

        // Give the foreign-universe node several announce periods on the shared group; its
        // datagrams are delivered, and only the inbound compare keeps it out of the A-nodes' tables.
        pump_until(io, [] { return false; }, k_settle);
        REQUIRE_FALSE(a1.router().known().contains(id_b));
        REQUIRE_FALSE(a2.router().known().contains(id_b));

        control_ms.push_back(ctrl_dt);
        WARN("[universe-isolation] run " << run << ": same-universe control " << ctrl_dt << " ms; foreign universe stayed unaware");
    }

    REQUIRE(control_ms.size() >= 3);
    WARN("[universe-isolation] same-host medians over " << control_ms.size() << " runs: control-awareness " << median_ms(control_ms) << " ms");
}

TEST_CASE("two hard-scoped same-universe nodes rendezvous on the derived group over real multicast "
          "even with an unparsable base group — the effective-group wiring proof",
          "[integration][discovery][universe]")
{
    constexpr int k_runs          = 3;
    const auto k_bound            = std::chrono::seconds(20);
    const std::string base        = "239.255.0.7";
    const std::uint16_t base_port = static_cast<std::uint16_t>(23000 + (plexus::testing::process_id() % 400) * 12);

    // The derived group is a real, distinct admin-scoped group — never the soft base nor the DDS
    // SPDP group. universe_group_octets excludes 239.255.0.* by range, so this holds by construction.
    const std::string derived = universe_group(universe_from_label("gamma"));
    REQUIRE(derived != "239.255.0.7");
    REQUIRE(derived != "239.255.0.1");

    std::vector<std::int64_t> hard_ms;

    for(int run = 0; run < k_runs; ++run)
    {
        ::asio::io_context io;
        pasio::default_discovery dc1{io, universe_opts("delta", universe_scoping::soft, base)};
        pasio::default_discovery dc2{io, universe_opts("delta", universe_scoping::soft, base)};
        // The unparsable group is the discriminator: hard derivation must ignore it. If the factory
        // still parsed options.group these two would fall back to 0.0.0.0 and never rendezvous.
        pasio::default_discovery dh1{io, universe_opts("gamma", universe_scoping::hard, "not-an-address")};
        pasio::default_discovery dh2{io, universe_opts("gamma", universe_scoping::hard, "not-an-address")};
        pasio::asio_transport tc1{io}, tc2{io}, th1{io}, th2{io};

        const auto id_c1 = make_id(0xC1);
        const auto id_c2 = make_id(0xC2);
        const auto id_h1 = make_id(0xD1);
        const auto id_h2 = make_id(0xD2);
        asio_node c1{io, dc1.discovery(), id_c1, tc1, make_opts()};
        asio_node c2{io, dc2.discovery(), id_c2, tc2, make_opts()};
        asio_node h1{io, dh1.discovery(), id_h1, th1, make_opts()};
        asio_node h2{io, dh2.discovery(), id_h2, th2, make_opts()};

        const std::uint16_t port = static_cast<std::uint16_t>(base_port + 4 * run);
        c1.listen({"tcp", "127.0.0.1:" + std::to_string(port)});
        c2.listen({"tcp", "127.0.0.1:" + std::to_string(port + 1)});
        h1.listen({"tcp", "127.0.0.1:" + std::to_string(port + 2)});
        h2.listen({"tcp", "127.0.0.1:" + std::to_string(port + 3)});

        // The soft same-universe pair on the base group is the liveness control: if it cannot
        // rendezvous the host has no usable loopback, so the hard failure would be the environment.
        const bool control = pump_until(io, [&] { return c1.router().known().contains(id_c2) && c2.router().known().contains(id_c1); }, k_bound);
        if(!control && run == 0)
            SKIP("multicast loopback unavailable on this host: the soft liveness control reached no awareness within the bound");
        REQUIRE(control);

        const auto t0      = std::chrono::steady_clock::now();
        const bool hard_aware = pump_until(io, [&] { return h1.router().known().contains(id_h2) && h2.router().known().contains(id_h1); }, k_bound);
        const auto hard_dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
        REQUIRE(hard_aware); // fails iff the factory ignores the effective group

        hard_ms.push_back(hard_dt);
        WARN("[universe-hard] run " << run << ": hard pair rendezvoused on " << derived << " in " << hard_dt << " ms (base group unparsable)");
    }

    REQUIRE(hard_ms.size() >= 3);
    WARN("[universe-hard] same-host medians over " << hard_ms.size() << " runs: hard-rendezvous " << median_ms(hard_ms) << " ms on derived group " << derived);
}
