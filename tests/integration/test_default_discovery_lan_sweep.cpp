#include "test_default_discovery_lan_common.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace default_discovery_lan_fixture;

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
