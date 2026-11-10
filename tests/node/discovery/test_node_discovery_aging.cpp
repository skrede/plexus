#include "test_node_discovery_aging_common.h"

#include <array>

using namespace node_discovery_aging_fixture;

TEST_CASE("node_discovery_aging: the recorded (announce_period, TTL) sweep justifies the locked default", "[node][discovery_aging]")
{
    const std::array<std::uint64_t, 3> announces{1 * k_sec, 2 * k_sec, 3 * k_sec};
    const std::array<std::uint64_t, 4> ttls{5 * k_sec, 10 * k_sec, 15 * k_sec, 30 * k_sec};
    constexpr int k_announce_count = 8;

    for(std::uint64_t announce : announces)
        for(std::uint64_t ttl : ttls)
        {
            const cell_outcome o = run_cell(announce, ttl, k_announce_count);
            INFO("announce=" << announce / k_sec << "s ttl=" << ttl / k_sec << "s -> stayed_known=" << o.stayed_known_while_announcing << " expired_after_silence="
                             << o.expired_after_silence << " expiry_within=" << o.expiry_within_ns / k_sec << "s missed_periods_tolerated=" << ttl / announce);

            // A steadily-announcing peer must NEVER false-expire (TTL > announce period).
            if(ttl > announce)
                REQUIRE(o.stayed_known_while_announcing);
            // A silenced peer must be forgotten, and within (TTL + one sweep granularity).
            REQUIRE(o.expired_after_silence);
            REQUIRE(o.expiry_within_ns <= ttl + k_tick_ns);
        }
}

TEST_CASE("node_discovery_aging: the locked 15s default tolerates several missed 1-3s announces", "[node][discovery_aging]")
{
    // The pinned default: 15s TTL survives 5 missed 3s periods (and 15 missed 1s periods), so a
    // single lost announce never expires a live peer, while a genuinely silent peer is gone <= 15s.
    REQUIRE(default_discovery_ttl_ns == 15 * k_sec);
    REQUIRE(default_discovery_ttl_ns / (3 * k_sec) >= 5);

    const cell_outcome at_3s = run_cell(3 * k_sec, default_discovery_ttl_ns, 8);
    REQUIRE(at_3s.stayed_known_while_announcing);
    REQUIRE(at_3s.expired_after_silence);
    REQUIRE(at_3s.expiry_within_ns <= default_discovery_ttl_ns + k_tick_ns);
}
