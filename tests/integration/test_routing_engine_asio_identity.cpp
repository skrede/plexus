#include "test_routing_engine_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace routing_asio_fixture;

TEST_CASE("routing over asio: one node dialing TWO peers near-simultaneously resolves each frame "
          "to its OWN source identity over real TCP",
          "[integration][routing][asio]")
{
    // The two-peer receive-path identity leg AND the dial-completion correlation
    // adversary: A dials B and C back-to-back, so both dials are in flight and their
    // TCP connects can complete OUT OF ORDER. The engine must route each completed
    // channel to ITS slot by the dial endpoint. If a channel were mis-correlated,
    // peer-B's published bytes would land in A's C-sink (or vice versa) — exactly the
    // cross-attribution this asserts against. Looped N to expose the race across
    // schedulings; the ctest invocation is re-run >=3 process runs.
    constexpr int     k_iterations = 100;
    const std::string from_b       = "payload-from-peer-b";
    const std::string from_c       = "payload-from-peer-c";
    int               proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false};
        asio_node          c{io, 0xC3, /*eager=*/false};
        const auto         id_b = make_id(0xB2);
        const auto         id_c = make_id(0xC3);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.note_peer(id_c, c.listen_ep());

        // Dial BOTH near-simultaneously: two overlapping in-flight dials.
        a.eng.subscribe(id_b, "topic-b");
        a.eng.subscribe(id_c, "topic-c");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && a.eng.is_connected(id_c); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.is_connected(id_c));

        auto *a_to_b = a.eng.session_for(id_b);
        auto *a_to_c = a.eng.session_for(id_c);
        REQUIRE(a_to_b != nullptr);
        REQUIRE(a_to_c != nullptr);

        // Each slot minted its OWN local epoch at handshake — a mis-correlation
        // would collapse the two slots onto one channel.
        REQUIRE(a_to_b->session_id() != 0);
        REQUIRE(a_to_c->session_id() != 0);

        // Per-slot sinks: B's bytes must reach the B-sink and C's the C-sink.
        std::vector<std::string> a_from_b, a_from_c;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d)
                           { a_from_b.emplace_back(to_string(d)); });
        a_to_c->on_message([&](std::string_view, std::span<const std::byte> d)
                           { a_from_c.emplace_back(to_string(d)); });

        const auto inbound1 = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        const auto inbound2 = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{2};
            return id;
        }();
        auto *b_in = b.eng.session_for(inbound1);
        auto *c_in = c.eng.session_for(inbound1); // each remote keys its sole inbound at index 1
        REQUIRE(b_in != nullptr);
        REQUIRE(c_in != nullptr);
        (void)inbound2;

        // A's two demand subscribes closed each loop over the real path: B's and C's
        // on_subscribe attached their producer-side fanout to A, so the manual attaches
        // are redundant — settle to let the subscribe round-trips wire the fanout.
        settle(io);

        b.eng.messages().publish("topic-b", as_bytes(from_b), b_in->session_id());
        c.eng.messages().publish("topic-c", as_bytes(from_c), c_in->session_id());
        pump_until(io, [&] { return !a_from_b.empty() && !a_from_c.empty(); });

        // Each frame resolved to its OWN source-peer identity — no cross-attribution.
        REQUIRE(a_from_b.size() == 1);
        REQUIRE(a_from_c.size() == 1);
        REQUIRE(a_from_b.front() == from_b);
        REQUIRE(a_from_c.front() == from_c);

        // The delivered frames latched each slot's PEER epoch to its own remote: A's
        // B-slot carries B's epoch and the C-slot carries C's. A mis-correlated
        // channel would latch the wrong peer's epoch (or collide both onto one).
        REQUIRE(a_to_b->peer_session_id() == b_in->session_id());
        REQUIRE(a_to_c->peer_session_id() == c_in->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("routing over asio: a dial that completes OUT OF ORDER is correlated to its own slot, "
          "not by arrival order",
          "[integration][routing][asio]")
{
    // The deterministic out-of-order adversary. A reaches B and C: B's listener is
    // DOWN at dial time (its dial is refused and the driver retries), while C's is up
    // and C connects IMMEDIATELY. So C's on_dialed fires FIRST — but A enqueued B's
    // slot FIRST (reach(B) before reach(C)). A by-arrival-order (FIFO) tail would pop
    // B's slot for C's channel: B's slot would carry C's connection and C's slot
    // would carry B's — a cross-attribution. The endpoint-correlated tail routes each
    // completed channel to ITS own slot. After C lands, B's listener is brought up on
    // its reserved port and B completes too; each slot must then resolve to its OWN
    // remote epoch over the wire. Looped to vary the scheduler interleavings.
    constexpr int     k_iterations = 40;
    const std::string from_b       = "out-of-order-from-b";
    const std::string from_c       = "out-of-order-from-c";
    int               proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          c{io, 0xC3, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false, /*listen_now=*/false};
        const auto         id_b = make_id(0xB2);
        const auto         id_c = make_id(0xC3);

        const auto b_port = reserve_closed_port(); // B's endpoint, listener still DOWN
        a.eng.note_peer(id_b, {"tcp", "127.0.0.1:" + std::to_string(b_port)});
        a.eng.note_peer(id_c, c.listen_ep());

        // Reach B FIRST (enqueued first; its dial is refused → retries) then C (up).
        a.eng.subscribe(id_b, "topic-b");
        a.eng.subscribe(id_c, "topic-c");

        // C completes first while B is still retrying — the out-of-order completion.
        pump_until(io, [&] { return a.eng.is_connected(id_c); });
        REQUIRE(a.eng.is_connected(id_c));
        REQUIRE(!a.eng.is_connected(id_b)); // B has NOT connected yet (listener down)

        // Bring B's listener up on its reserved port; B's retry now succeeds.
        b.listen_on(b_port);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));

        auto *a_to_b = a.eng.session_for(id_b);
        auto *a_to_c = a.eng.session_for(id_c);
        REQUIRE(a_to_b != nullptr);
        REQUIRE(a_to_c != nullptr);

        std::vector<std::string> a_from_b, a_from_c;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d)
                           { a_from_b.emplace_back(to_string(d)); });
        a_to_c->on_message([&](std::string_view, std::span<const std::byte> d)
                           { a_from_c.emplace_back(to_string(d)); });

        const auto inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_in = b.eng.session_for(inbound);
        auto *c_in = c.eng.session_for(inbound);
        REQUIRE(b_in != nullptr);
        REQUIRE(c_in != nullptr);

        // A's two demand subscribes closed each loop over the real path: B's and C's
        // on_subscribe attached their producer-side fanout to A, so the manual attaches
        // are redundant — settle to let the subscribe round-trips wire the fanout.
        settle(io);

        b.eng.messages().publish("topic-b", as_bytes(from_b), b_in->session_id());
        c.eng.messages().publish("topic-c", as_bytes(from_c), c_in->session_id());
        pump_until(io, [&] { return !a_from_b.empty() && !a_from_c.empty(); });

        // No cross-attribution: B's bytes reached the B-slot and C's the C-slot, and
        // each slot latched its OWN remote epoch — even though C's channel completed
        // BEFORE B's despite B's slot being enqueued first.
        REQUIRE(a_from_b.size() == 1);
        REQUIRE(a_from_c.size() == 1);
        REQUIRE(a_from_b.front() == from_b);
        REQUIRE(a_from_c.front() == from_c);
        REQUIRE(a_to_b->peer_session_id() == b_in->session_id());
        REQUIRE(a_to_c->peer_session_id() == c_in->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
