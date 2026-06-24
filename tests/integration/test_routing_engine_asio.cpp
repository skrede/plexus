#include "test_routing_engine_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace routing_asio_fixture;

TEST_CASE("routing over asio: LAZY opens no connection until a demand call, then dials and "
          "completes over real TCP",
          "[integration][routing][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false};
        const auto         id_b = make_id(0xB2);

        // Awareness alone: no demand, so the lazy engine dials NOTHING.
        a.eng.note_peer(id_b, b.listen_ep());
        settle(io);
        REQUIRE(!a.eng.has_session(id_b));

        // Demand: reach the known-but-unconnected peer. NOW it dials over real TCP,
        // the inbound bootstrap accepts, and the handshake completes both ends.
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.session_for(id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("routing over asio: a demand subscribe carries a real published message with the minted "
          "epoch over real TCP",
          "[integration][routing][asio]")
{
    constexpr int     k_iterations = 100;
    const std::string payload      = "routed-published-bytes-over-tcp";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/false};
        asio_node          b{io, 0xB2, /*eager=*/false};
        const auto         id_b = make_id(0xB2);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));

        // A's demand subscribe closes the loop over the real path: the resurrected
        // subscribe reaches B, whose on_subscribe attaches B's producer-side fanout to
        // A (so the manual attach is no longer needed — the loop is genuinely closed).
        // B publishes; the frame resolves through A's own node-shared forwarder to A's
        // per-session sink carrying B's minted epoch.
        auto *a_to_b = a.eng.session_for(id_b);
        REQUIRE(a_to_b != nullptr);
        std::vector<std::string> a_received;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(to_string(d)); });

        const auto inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_inbound = b.eng.session_for(inbound);
        REQUIRE(b_inbound != nullptr);
        settle(io); // drain the subscribe round-trip so B's fanout to A is wired

        b.eng.messages().publish("topic", as_bytes(payload), b_inbound->session_id());
        pump_until(io, [&] { return !a_received.empty(); });
        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == payload);
        REQUIRE(a_to_b->peer_session_id() == b_inbound->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("routing over asio: EAGER dials and completes off note_peer ALONE with no demand call "
          "over real TCP",
          "[integration][routing][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node          a{io, 0xA1, /*eager=*/true};
        asio_node          b{io, 0xB2, /*eager=*/true};
        const auto         id_b = make_id(0xB2);

        // No reach/subscribe/call: awareness ALONE triggers the dial+handshake.
        a.eng.note_peer(id_b, b.listen_ep());
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.session_for(id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
