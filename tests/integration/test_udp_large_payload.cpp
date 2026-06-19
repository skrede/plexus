#include "test_udp_large_payload_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_large_payload_fixture;

TEST_CASE("loss_shim drops a fixed-seed fraction deterministically across two runs", "[loss_shim]")
{
    // Two schedulers with the SAME config + seed must produce a byte-identical drop/emit
    // sequence over a fixed datagram stream — the reproducibility law the empirical sweeps
    // and the lossy benchmark cell rely on (no std::random anywhere in the path).
    ptest::loss_reorder_config cfg{
            .loss_num = 30, .loss_den = 100, .reorder_depth = 4, .seed = 0xABCDEF01u};

    auto run_once = [&]
    {
        ptest::loss_reorder_scheduler       sched{cfg};
        std::vector<std::vector<std::byte>> emitted;
        for(int i = 0; i < 500; ++i)
        {
            std::vector<std::byte> dg{static_cast<std::byte>(i & 0xFF),
                                      static_cast<std::byte>((i >> 8) & 0xFF)};
            for(auto &out : sched.drive(dg))
                emitted.push_back(std::move(out));
        }
        for(auto &out : sched.flush())
            emitted.push_back(std::move(out));
        return std::pair{std::move(emitted), sched.dropped()};
    };

    auto [seq_a, dropped_a] = run_once();
    auto [seq_b, dropped_b] = run_once();

    REQUIRE(dropped_a == dropped_b); // identical drop COUNT across runs
    REQUIRE(dropped_a > 0);          // the loss fraction genuinely engaged
    REQUIRE(seq_a.size() == seq_b.size());
    for(std::size_t i = 0; i < seq_a.size(); ++i)
        REQUIRE(seq_a[i] == seq_b[i]); // byte-identical emit ORDER (drop + reorder)

    // The reorder window genuinely reordered (the emitted order is not the input order).
    bool reordered = false;
    for(std::size_t i = 1; i < seq_a.size(); ++i)
        if(seq_a[i] < seq_a[i - 1])
        {
            reordered = true;
            break;
        }
    REQUIRE(reordered);
}

TEST_CASE("loss_shim with zero loss and zero reorder is an order-preserving pass-through",
          "[loss_shim]")
{
    ptest::loss_reorder_scheduler       sched{ptest::loss_reorder_config{}};
    std::vector<std::vector<std::byte>> emitted;
    for(int i = 0; i < 64; ++i)
    {
        std::vector<std::byte> dg{static_cast<std::byte>(i)};
        for(auto &out : sched.drive(dg))
            emitted.push_back(std::move(out));
    }
    REQUIRE(emitted.size() == 64);
    REQUIRE(sched.dropped() == 0);
    for(int i = 0; i < 64; ++i)
        REQUIRE(emitted[static_cast<std::size_t>(i)].front() == static_cast<std::byte>(i));
}
