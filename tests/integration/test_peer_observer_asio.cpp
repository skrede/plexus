#include "test_peer_observer_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace observer_asio_fixture;

TEST_CASE("observer over asio: connected fires once and ready fires immediately for a "
          "zero-subscribe peer over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false};
        const auto         id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(c.connected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.ready == 1);
        REQUIRE(c.last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: a real socket drop+redial fires reconnected (NOT a second "
          "connected) and a disconnected over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false};
        const auto         id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(rec.for_peer(id_b).connected == 1);

        // A REAL transport drop: close B's accepted socket so A observes the drop and
        // re-dials over a fresh socket, re-handshaking. reconnected fires (not a second
        // connected); the dead session's tear_down fires disconnected.
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io,
                   [&] { return rec.for_peer(id_b).reconnected == 1 && a.eng.is_connected(id_b); });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(c.connected == 1);
        REQUIRE(c.reconnected == 1);
        REQUIRE(c.disconnected == 1);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: dead fires once when the driver surrenders against an unbindable "
          "endpoint over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        // A dials a reserved-then-closed port that never binds: every dial is refused,
        // so the bounded driver surrenders and fires dead.
        asio_node          a{io, 0xA1, /*eager=*/false, bounded_cfg(/*max_attempts=*/3)};
        const auto         id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        const auto dead_port = reserve_closed_port();
        a.eng.note_peer(id_b, {"tcp", "127.0.0.1:" + std::to_string(dead_port)});
        a.eng.reach(id_b);
        pump_until(io, [&] { return rec.for_peer(id_b).dead == 1; });

        REQUIRE(a.eng.is_dead(id_b));
        REQUIRE(rec.for_peer(id_b).dead == 1);
        REQUIRE(rec.for_peer(id_b).connected == 0); // never connected
        REQUIRE(rec.for_peer(id_b).last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: rejected fires once carrying the real refusal reason on a "
          "version-incompatible handshake over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        // A requires compatible >= 2; B advertises version 1, so A rejects B's accept
        // response over the wire -> A fires rejected(reject_version). A's redial is
        // bounded so a rejected (protocol-error) close does not spin forever.
        asio_node          a{io, 0xA1, /*eager=*/false, bounded_cfg(2), /*compatible=*/2};
        asio_node          b{io, 0xB2, /*eager=*/false};
        const auto         id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return rec.for_peer(id_b).rejected >= 1; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(c.rejected >= 1);
        REQUIRE(c.last_reason == handshake_outcome::reject_version);
        REQUIRE(c.connected == 0);
        REQUIRE(!a.eng.is_connected(id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
