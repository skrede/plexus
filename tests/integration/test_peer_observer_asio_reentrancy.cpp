#include "test_peer_observer_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace observer_asio_fixture;

TEST_CASE("observer over asio: an accepted (inbound) peer fires connected/disconnected/ready but "
          "NEVER reconnected or dead over real TCP",
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
        b.eng.add_observer(rec); // observe the ACCEPTING node's inbound slot

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        const auto inbound = inbound_slot(1);
        pump_until(io, [&] { return rec.for_peer(inbound).connected == 1 && rec.for_peer(inbound).ready == 1; });
        {
            const auto &c = rec.for_peer(inbound);
            REQUIRE(c.connected == 1);
            REQUIRE(c.ready == 1);
            REQUIRE(c.last_kind == peer_kind::accepted);
        }

        // Drop from A's side: B's accepted session tears down (disconnected) but owns
        // no driver, so NEVER reconnect/dead.
        a.eng.session_for(id_b)->tear_down();
        pump_until(io, [&] { return rec.for_peer(inbound).disconnected == 1; });
        settle(io); // give any (erroneous) reconnect/dead a chance to fire

        const auto &c = rec.for_peer(inbound);
        REQUIRE(c.disconnected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.dead == 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: calling engine.subscribe from inside an observer callback is "
          "posted-safe over real TCP (no re-entrancy crash)",
          "[integration][observer][asio]")
{
    struct reentrant_observer final : public plexus::io::observer
    {
        engine *eng{nullptr};
        plexus::node_id target{};
        int connected{0};
        void on_peer_connected(const plexus::node_id &, std::string_view, peer_kind) override
        {
            ++connected;
            if(eng)
                eng->subscribe(target, "late-topic");
        }
    };

    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        reentrant_observer obs;
        obs.eng    = &a.eng;
        obs.target = id_b;
        a.eng.add_observer(obs);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return obs.connected == 1; });
        settle(io); // the nested subscribe runs on a later turn — no crash

        REQUIRE(obs.connected == 1);
        REQUIRE(a.eng.is_connected(id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
