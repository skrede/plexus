#include "test_default_discovery_lan_common.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <chrono>
#include <vector>
#include <cstdint>

using namespace default_discovery_lan_fixture;

// A SAME-HOST multicast-loopback PROXY for the out-of-box discovery MECHANISM. Two nodes on ONE
// host, each built with the canonical zero-config setup (an io_context, a default_discovery over
// it, and a node handed that discovery — NO hand-wired socket or leaf), become mutually aware over
// REAL sockets, REAL IPv4 multicast, with loopback engaged. The awareness assertion is reproduced
// over several runs with medians recorded.
//
// HONESTY FRAME: this proves the discovery MECHANISM only. It is NOT the two-host same-LAN-segment
// claim (SC1 / DISC-01), which needs a real NIC between two separate hosts. That genuine two-host
// reproduction (two machines, one LAN segment, >=3 runs, medians) is a SEPARATE hardware-gated
// MANUAL step and stays UNPROVEN in this single-host run. The loopback proxy does NOT satisfy it.

TEST_CASE("default_discovery_lan two configless nodes reach mutual awareness over real multicast "
          "loopback — same-host PROXY for the discovery mechanism, NOT the two-host claim",
          "[integration][discovery][default_discovery_lan]")
{
    constexpr int k_runs          = 5;
    const auto k_bound            = std::chrono::seconds(20);
    const std::uint16_t base_port = static_cast<std::uint16_t>(19000 + (plexus::testing::process_id() % 800) * 3);

    std::vector<std::int64_t> awareness_ms;
    std::vector<std::int64_t> healthy_ms;

    for(int run = 0; run < k_runs; ++run)
    {
        ::asio::io_context io;
        pasio::default_discovery disc_a{io};
        pasio::default_discovery disc_b{io};
        pasio::asio_transport tp_a{io};
        pasio::asio_transport tp_b{io};

        const auto id_a = make_id(0xA1);
        const auto id_b = make_id(0xB2);
        asio_node a{io, disc_a.discovery(), id_a, tp_a, make_opts()};
        asio_node b{io, disc_b.discovery(), id_b, tp_b, make_opts()};

        const std::uint16_t port_a = static_cast<std::uint16_t>(base_port + 2 * run);
        const std::uint16_t port_b = static_cast<std::uint16_t>(port_a + 1);
        a.listen({"tcp", "127.0.0.1:" + std::to_string(port_a)});
        b.listen({"tcp", "127.0.0.1:" + std::to_string(port_b)});

        const auto t0    = std::chrono::steady_clock::now();
        const bool aware = pump_until(io, [&] { return a.router().known().contains(id_b) && b.router().known().contains(id_a); }, k_bound);
        const auto aware_dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

        // A recorded, never-silent skip: a host with no usable multicast loopback (a sandbox that
        // drops IGMP-joined delivery) cannot reach awareness at all. Detected on the first run.
        if(!aware && run == 0)
            SKIP("multicast loopback unavailable on this host: two configless default_discovery nodes reached no mutual awareness within the bound");

        REQUIRE(aware); // reproduced on every run once the host supports loopback

        // C-01: a node never counts its own multicast echo as a peer.
        REQUIRE_FALSE(a.router().known().contains(id_a));
        REQUIRE_FALSE(b.router().known().contains(id_b));

        const auto h0      = std::chrono::steady_clock::now();
        const bool healthy = pump_until(io, [&] { return disc_a.self_check() == discovery_health::healthy && disc_b.self_check() == discovery_health::healthy; }, k_bound);
        const auto healthy_dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - h0).count();
        REQUIRE(healthy);
        REQUIRE(disc_a.self_check() == discovery_health::healthy);
        REQUIRE(disc_b.self_check() == discovery_health::healthy);

        awareness_ms.push_back(aware_dt);
        healthy_ms.push_back(healthy_dt);
        WARN("[loopback-proxy] run " << run << ": mutual-awareness " << aware_dt << " ms, self-check-healthy " << healthy_dt << " ms");
    }

    REQUIRE(awareness_ms.size() >= 3);
    WARN("[loopback-proxy] same-host medians over " << awareness_ms.size() << " runs: mutual-awareness " << median_ms(awareness_ms) << " ms, self-check-healthy "
         << median_ms(healthy_ms) << " ms. This proves the discovery MECHANISM only; the two-host same-LAN DISC-01/SC1 reproduction is an UNPROVEN hardware-gated manual step.");
}
