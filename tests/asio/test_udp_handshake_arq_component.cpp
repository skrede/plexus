#include "test_udp_handshake_arq_common.h"

using namespace udp_handshake_arq_fixture;

TEST_CASE("udp handshake_arq component: the ladder retransmits and a paired frame cancels it", "[udp][handshake]")
{
    using arq_t = plexus::datagram::detail::udp_handshake_arq<pasio::udp_policy>;

    SECTION("a paired frame mid-ladder establishes and stops further transmits")
    {
        ::asio::io_context io;
        arq_t              arq{io, fast_ladder};

        int  transmits   = 0;
        bool established = false;
        bool timed_out   = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_established([&] { established = true; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start(); // transmit #1 fires immediately
        REQUIRE(transmits == 1);
        pump_until(io, [&] { return transmits >= 2; }); // the 20ms timer fires transmit #2
        REQUIRE(transmits >= 2);

        arq.on_paired_frame(); // the response arrives
        REQUIRE(established);
        int at_pairing = transmits;
        pump_until(io, [&] { return false; }, ms{120}); // let the would-be ladder elapse
        REQUIRE(transmits == at_pairing);               // no further transmit after resolution
        REQUIRE_FALSE(timed_out);
    }

    SECTION("exhausting the ladder surfaces a timeout abort")
    {
        ::asio::io_context io;
        arq_t              arq{io, fast_ladder};

        int  transmits = 0;
        bool timed_out = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start();
        pump_until(io, [&] { return timed_out; });
        REQUIRE(timed_out);
        REQUIRE(transmits == 3); // the three ladder attempts, then surrender
    }

    SECTION("single-owner teardown cancels a pending timer cleanly")
    {
        ::asio::io_context io;
        {
            arq_t arq{io, fast_ladder};
            arq.on_transmit([] {});
            arq.start();  // a timer is pending
            arq.cancel(); // the owner tears it down
            REQUIRE(arq.resolved());
        }
        io.poll(); // a cancelled timer must not fire on a dead ARQ
        SUCCEED();
    }
}
