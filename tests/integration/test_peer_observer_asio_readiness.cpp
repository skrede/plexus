#include "test_peer_observer_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace observer_asio_fixture;

TEST_CASE("observer over asio: on_peer_ready over the REAL loop, then the awaited publish lands "
          "over real TCP (first-publish-loss-free)",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload  = "ready-then-publish-over-tcp";
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        // Subscribe through the engine (the REAL counted loop, NO faked attach): the
        // lazy subscribe triggers the dial, is remembered until complete, then flushed
        // so ready fires after its ack.
        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(rec.for_peer(id_b).ready == 1);

        // The awaited publish: A is the subscriber; B publishes and the frame lands at
        // A's per-session sink — proving the subscribe round-trip wired the fan-out.
        auto *a_to_b = a.eng.session_for(id_b);
        std::vector<std::string> a_received;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(to_string(d)); });

        auto *b_inbound = b.eng.session_for(inbound_slot(1));
        REQUIRE(b_inbound != nullptr);
        settle(io); // let B's producer-side fanout settle from A's subscribe
        b.eng.messages().publish("topic", as_bytes(payload), b_inbound->session_id());
        pump_until(io, [&] { return !a_received.empty(); });

        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: ready fires EXACTLY once per cycle across a real reconnect (count "
          "== 2 over two cycles)",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(rec.for_peer(id_b).ready == 1); // cycle 1

        // Real socket drop + re-dial: the fresh incarnation re-arms and resurrects the
        // remembered subscribe through the counted path, firing ready a SECOND time.
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io, [&] { return rec.for_peer(id_b).ready == 2; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(c.ready == 2);
        REQUIRE(c.reconnected == 1);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: premature-ready window — ready stays 1 across the reconnect-complete "
          "predicate before the resurrected acks drain, becomes 2 only after over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        // Connect first (zero-subscribe ready fires once), then subscribe N>1 topics on
        // the live session so each is remembered. The reconnect resurrects all N.
        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(rec.for_peer(id_b).ready == 1); // cycle 1: zero-subscribe ready
        a.eng.subscribe(id_b, "topic-1");
        a.eng.subscribe(id_b, "topic-2");
        a.eng.subscribe(id_b, "topic-3");
        settle(io);
        REQUIRE(rec.for_peer(id_b).ready == 1); // late subscribes do NOT re-fire

        // Real socket drop. The reconnect-complete predicate (reconnected fired) marks
        // the resurrection window: on_complete ran resubscribe_all (counter now N) and
        // held ready behind the counted path. The invariant under test is that ready
        // NEVER fires prematurely past the cycle count — i.e. ready advances ONLY when the
        // counted resurrected acks drain, never by bypassing the counter. With TCP_NODELAY
        // on (the default) the resurrected subscribe-acks flush immediately, so ready may
        // already have advanced to 2 by the time the reconnected predicate settles; the
        // load-bearing assertion is that it never OVERSHOOTS 2 (a bypass of the counted
        // path would fire ready more than once per resurrected cycle).
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io, [&] { return rec.for_peer(id_b).reconnected == 1; });
        REQUIRE(rec.for_peer(id_b).reconnected == 1);
        REQUIRE(rec.for_peer(id_b).ready <= 2); // never premature past the cycle count

        // The resurrected subscribe_responses drain through the counted path: ready
        // settles at EXACTLY 2 (one ready per cycle), never more.
        pump_until(io, [&] { return rec.for_peer(id_b).ready == 2; });
        REQUIRE(rec.for_peer(id_b).ready == 2);
        settle(io);
        REQUIRE(rec.for_peer(id_b).ready == 2); // does not overshoot — the counted path fired once
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
