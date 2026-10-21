#include "queued_teardown.h"
#include "test_udp_handshake_arq_common.h"

#include <cstdint>

using namespace udp_handshake_arq_fixture;

using arq_t = pasio::udp_transport::arq_type;

TEST_CASE("udp handshake_arq component: the ladder retransmits and a paired frame cancels it", "[udp][handshake]")
{
    SECTION("a paired frame mid-ladder establishes and stops further transmits")
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};

        int transmits    = 0;
        bool established = false;
        bool timed_out   = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_established([&] { established = true; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start(); // transmit #1 fires immediately
        REQUIRE(transmits == 1);
        plexus::testing::pump_until(io, [&] { return transmits >= 2; }, k_pump_bound); // the 20ms timer fires transmit #2
        REQUIRE(transmits >= 2);

        arq.on_paired_frame(); // the response arrives
        REQUIRE(established);
        int at_pairing = transmits;
        plexus::testing::pump_until(io, [&] { return false; }, ms{120}); // let the would-be ladder elapse
        REQUIRE(transmits == at_pairing);                                // no further transmit after resolution
        REQUIRE_FALSE(timed_out);
    }

    SECTION("exhausting the ladder surfaces a timeout abort")
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};

        int transmits  = 0;
        bool timed_out = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start();
        plexus::testing::pump_until(io, [&] { return timed_out; }, k_pump_bound);
        REQUIRE(timed_out);
        REQUIRE(transmits == 3); // the three ladder attempts, then surrender
    }
}

TEST_CASE("udp handshake_arq component: cancel settles the wait count once, after the last aborted wait completed", "[udp][handshake]")
{
    ::asio::io_context io;
    arq_t arq{io, fast_ladder};
    std::int32_t settled = 0;
    arq.pending().on_settled([&settled] { ++settled; });
    arq.on_transmit([] {});
    arq.start();
    REQUIRE(arq.pending().count() == 1);

    arq.cancel();
    REQUIRE(arq.resolved());
    REQUIRE(settled == 0);
    io.poll();
    REQUIRE(settled == 1);
    REQUIRE(arq.pending().count() == 0);
}

// The resolving action runs in the reactor pass that already queued the first retransmit expiry.
TEST_CASE("udp handshake_arq component: a handshake ARQ whose retransmit expiry is already queued when it resolves does not retransmit", "[udp][handshake]")
{
    for(std::int32_t i = 0; i < plexus::testing::k_teardown_iterations; ++i)
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};
        std::int32_t transmits = 0;
        arq.on_transmit([&transmits] { ++transmits; });
        arq.start();

        plexus::testing::queued_action resolving{io, [&arq] { arq.on_paired_frame(); }};
        resolving.run_after(ms{40});

        CHECK(transmits == 1);
    }
}

TEST_CASE("udp handshake_arq component: a handshake ARQ whose retransmit expiry is already queued when it is canceled does not retransmit", "[udp][handshake]")
{
    for(std::int32_t i = 0; i < plexus::testing::k_teardown_iterations; ++i)
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};
        std::int32_t transmits = 0;
        arq.on_transmit([&transmits] { ++transmits; });
        arq.start();

        plexus::testing::queued_action canceling{io, [&arq] { arq.cancel(); }};
        canceling.run_after(ms{40});

        CHECK(transmits == 1);
    }
}
