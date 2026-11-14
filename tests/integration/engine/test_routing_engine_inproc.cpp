#include "test_routing_engine_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace routing_inproc_fixture;

TEST_CASE("inproc routing: note_peer records awareness and dials NOTHING (awareness without connect)", "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net; // lazy (default): no eager dial off awareness

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        REQUIRE(net.a.known().contains(net.id_b));
        REQUIRE(net.a.known().lookup(net.id_b).has_value());
        REQUIRE(*net.a.known().lookup(net.id_b) == net.ep_b);
        REQUIRE(!net.a.has_session(net.id_b)); // awareness opened no connection
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: LAZY (default) opens no session until a demand call, then dials and "
          "completes",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net; // lazy default

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b)); // no demand yet -> no dial

        // Demand: reach the known-but-unconnected peer. NOW it dials, the inbound
        // bootstrap accepts, and the handshake completes on both ends.
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: a demand subscribe (not just reach) dials a known-but-unconnected peer "
          "and completes",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b));

        net.a.subscribe(net.id_b, "topic");
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: EAGER (opt-in knob) dials and completes off note_peer ALONE, with no "
          "demand call",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net(/*eager=*/true);

        // No reach/subscribe/call: awareness ALONE triggers the dial+handshake.
        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: an engine destroyed without close leaves its still-listening transport accepting into no "
          "slot",
          "[integration][routing][inproc]")
{
    manual_clock::reset();
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};
    plexus::log::null_logger sink;
    const endpoint ep_b{"inproc", "node-b"};
    engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink);
    auto b = std::make_unique<engine>(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink);
    b->listen(ep_b);
    ex.drain();
    b.reset();

    a.dial(ep_b);
    REQUIRE(transport_b.retiring_size() == 1);
    ex.drain();
    REQUIRE(transport_b.retiring_size() == 0);
}
