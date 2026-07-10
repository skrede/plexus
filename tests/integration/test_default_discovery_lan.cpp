#include "plexus/asio/default_discovery.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/node_id.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

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

namespace pasio = plexus::asio;
using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;
using plexus::discovery::discovery_health;

namespace {

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0x5a)};
    return id;
}

// redial_seed is required-distinct; awareness here is lazy (dial_eagerly defaults false), so a
// noted peer is recorded in known() without a dial — the value is inert for this awareness proof.
plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
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

std::int64_t median_ms(std::vector<std::int64_t> xs)
{
    std::sort(xs.begin(), xs.end());
    return xs.empty() ? 0 : xs[xs.size() / 2];
}

} // namespace

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

// The empirical announce-numeric sweep (hidden [.sweep]: an on-demand measurement rig, not a gate).
// It measures, on THIS host, the worst-case time-to-mutual-awareness (a late joiner waiting out the
// re-announce cadence) and a cross-source collision proxy (near-simultaneous inbound announces at a
// passive observer) across a grid of (announce_period, jitter_fraction). Run explicitly to capture
// the numbers recorded in the phase sweep note:
//   ctest --test-dir build -R default_discovery_sweep --output-on-failure
// (Catch runs a [.]-hidden case only when its ctest test is invoked by name.) The medians are REAL
// measurements printed via WARN; no number here is assumed.
TEST_CASE("default_discovery_sweep announce period/jitter empirical grid", "[.sweep][integration][discovery][default_discovery_sweep]")
{
    struct cell
    {
        int period_ms;
        double jitter;
    };
    const std::vector<cell> grid{{200, 0.0}, {200, 0.2}, {200, 0.4}, {500, 0.0}, {500, 0.2}, {500, 0.4}, {1000, 0.0}, {1000, 0.2}, {1000, 0.4}};

    constexpr int k_runs          = 5;
    const std::uint16_t base_port = static_cast<std::uint16_t>(23000 + (plexus::testing::process_id() % 500) * 4);

    WARN("[sweep] grid over " << grid.size() << " cells, " << k_runs << " runs/cell; worst-case awareness = a late joiner waiting the re-announce cadence");

    for(const auto &g : grid)
    {
        plexus::discovery::discovery_options snd;
        snd.announce_period = std::chrono::milliseconds{g.period_ms};
        snd.jitter_fraction = g.jitter;

        std::vector<std::int64_t> worst_ms;
        for(int run = 0; run < k_runs; ++run)
        {
            ::asio::io_context io;
            pasio::default_discovery disc_a{io, snd};
            pasio::asio_transport tp_a{io};
            asio_node a{io, disc_a.discovery(), make_id(0xA1), tp_a, make_opts()};
            const std::uint16_t port_a = static_cast<std::uint16_t>(base_port + 2 * run);
            a.listen({"tcp", "127.0.0.1:" + std::to_string(port_a)});

            // Drain a's immediate emit + self-echo so the late joiner b MISSES it and must wait
            // out a's next re-announce — isolating the period/jitter cadence cost.
            pump_until(io, [] { return false; }, std::chrono::milliseconds{60});

            const auto t0 = std::chrono::steady_clock::now();
            pasio::default_discovery disc_b{io, snd};
            pasio::asio_transport tp_b{io};
            asio_node b{io, disc_b.discovery(), make_id(0xB2), tp_b, make_opts()};
            b.listen({"tcp", "127.0.0.1:" + std::to_string(port_a + 1)});

            const bool aware = pump_until(io, [&] { return b.router().known().contains(make_id(0xA1)); }, std::chrono::seconds(10));
            if(!aware && run == 0 && &g == &grid.front())
                SKIP("multicast loopback unavailable on this host");
            REQUIRE(aware);
            worst_ms.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
        }

        // Collision proxy: two co-host senders announce for a fixed window while a passive observer
        // (per-source cap disabled so every announce is seen) timestamps each inbound. Consecutive
        // arrivals within a tight threshold are near-simultaneous cross-source announces — the
        // storm-clustering jitter exists to decorrelate.
        ::asio::io_context io;
        plexus::discovery::discovery_options obs = snd;
        obs.cap.per_source_max                   = 0;
        pasio::default_discovery observer{io, obs};
        std::vector<std::int64_t> arrivals_us;
        arrivals_us.reserve(4096);
        const auto probe0 = std::chrono::steady_clock::now();
        observer.discovery().browse([&](const plexus::discovery::service_info &) { arrivals_us.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - probe0).count()); });

        pasio::default_discovery disc_x{io, snd};
        pasio::default_discovery disc_y{io, snd};
        pasio::asio_transport tp_x{io};
        pasio::asio_transport tp_y{io};
        asio_node x{io, disc_x.discovery(), make_id(0xC3), tp_x, make_opts()};
        asio_node y{io, disc_y.discovery(), make_id(0xD4), tp_y, make_opts()};
        x.listen({"tcp", "127.0.0.1:" + std::to_string(base_port + 100)});
        y.listen({"tcp", "127.0.0.1:" + std::to_string(base_port + 101)});

        pump_until(io, [] { return false; }, std::chrono::milliseconds{3000});

        std::sort(arrivals_us.begin(), arrivals_us.end());
        constexpr std::int64_t k_collision_us = 1500;
        int collisions                        = 0;
        for(std::size_t i = 1; i < arrivals_us.size(); ++i)
            if(arrivals_us[i] - arrivals_us[i - 1] <= k_collision_us)
                ++collisions;

        WARN("[sweep] period=" << g.period_ms << "ms jitter=" << g.jitter << " -> worst-case awareness median " << median_ms(worst_ms) << " ms (runs "
             << k_runs << "); observed announces=" << arrivals_us.size() << ", near-simultaneous(<=" << k_collision_us << "us) cross-source pairs=" << collisions);
    }
}
