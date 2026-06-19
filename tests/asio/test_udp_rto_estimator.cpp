// The adaptive RTO estimator unit (RFC 6298): the SRTT/RTTVAR smoothing converges
// toward the sampled RTT, the RTO stays clamped to [min, max], and the Karn
// multiplicative backoff doubles per retransmit (capped). A pure value type — no timer,
// no socket — so the smoothing math is proven in isolation from the IO engine.

#include "plexus/io/detail/udp_rto_estimator.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

namespace detail = plexus::io::detail;
using ms         = std::chrono::milliseconds;

TEST_CASE("udp rto: the first sample seeds SRTT=R, RTTVAR=R/2 and clamps the RTO", "[udp][rto]")
{
    detail::udp_rto_estimator est{ms{200}, ms{50}, ms{2000}};
    REQUIRE_FALSE(est.seeded());
    REQUIRE(est.rto() == ms{200}); // initial, pre-sample

    est.sample(ms{100}); // SRTT=100, RTTVAR=50 -> RTO=100+200=300
    REQUIRE(est.seeded());
    REQUIRE(est.rto() == ms{300});
}

TEST_CASE("udp rto: repeated steady samples converge the RTO toward the RTT band", "[udp][rto]")
{
    detail::udp_rto_estimator est{ms{200}, ms{10}, ms{2000}};
    for(int i = 0; i < 50; ++i)
        est.sample(ms{40}); // a steady 40ms path

    // SRTT -> 40, RTTVAR -> 0; RTO -> ~40, well inside [10, 2000] and far below the
    // 200ms conservative initial (the estimator adapted DOWN to the fast path).
    REQUIRE(est.rto() >= ms{38});
    REQUIRE(est.rto() <= ms{60});
}

TEST_CASE("udp rto: the RTO is clamped to the configured floor and ceiling", "[udp][rto]")
{
    detail::udp_rto_estimator floor{ms{200}, ms{500}, ms{2000}};
    floor.sample(ms{1}); // tiny RTT -> RTO would be ~2ms, clamped UP to min
    REQUIRE(floor.rto() == ms{500});

    detail::udp_rto_estimator ceil{ms{200}, ms{10}, ms{300}};
    ceil.sample(ms{5000}); // huge RTT -> RTO clamped DOWN to max
    REQUIRE(ceil.rto() == ms{300});
}

TEST_CASE("udp rto: Karn backoff doubles per retransmit and caps at max", "[udp][rto]")
{
    detail::udp_rto_estimator est{ms{100}, ms{10}, ms{500}};
    REQUIRE(est.backed_off(0) == ms{100}); // no retransmit -> base RTO
    REQUIRE(est.backed_off(1) == ms{200}); // x2
    REQUIRE(est.backed_off(2) == ms{400}); // x4
    REQUIRE(est.backed_off(3) == ms{500}); // x8 would be 800 -> capped at max=500
    REQUIRE(est.backed_off(8) == ms{500}); // stays capped
}
