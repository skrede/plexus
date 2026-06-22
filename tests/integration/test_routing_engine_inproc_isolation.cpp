#include "test_routing_engine_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace routing_inproc_fixture;

TEST_CASE("inproc routing: a published message resolves to its own engine's receive sink "
          "(receive-path identity)",
          "[integration][routing][inproc]")
{
    manual_clock::reset();
    two_node net(/*eager=*/true);
    net.discovery.announce(net.a, net.id_b, net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(net.id_b));

    auto *a_to_b = net.a.session_for(net.id_b);
    REQUIRE(a_to_b != nullptr);

    // B accepted one inbound slot; find it and wire its sink. The inbound session is
    // the one B holds with a complete handshake.
    plexus::node_id inbound = make_id(0x00);
    inbound[15]             = std::byte{1};
    auto *b_inbound         = net.b.session_for(inbound);
    REQUIRE(b_inbound != nullptr);
    REQUIRE(b_inbound->is_complete());

    std::vector<std::string> b_received;
    b_inbound->on_message([&](std::string_view, std::span<const std::byte> d)
                          { b_received.emplace_back(to_string(d)); });

    // B subscribes to A's topic (producer-side fanout), A publishes. The message
    // resolves through B's OWN node-shared forwarder to B's sink.
    REQUIRE(net.b.messages().attach_for_fanout(b_inbound->msg_peer(), "topic"));
    REQUIRE(net.a.messages().attach_for_fanout(a_to_b->msg_peer(), "topic"));
    net.drive();
    net.a.messages().publish("topic", as_bytes(std::string{"hello-b"}), a_to_b->session_id());
    net.drive();

    REQUIRE(b_received.size() == 1);
    REQUIRE(b_received.front() == "hello-b");
}

TEST_CASE("inproc routing: a publish to a known-but-unconnected peer's topic opens NO connection "
          "(publish does not dial)",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net; // lazy

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        // Publish to a topic naming no connected subscriber: it must NOT dial.
        net.a.publish("topic", as_bytes(std::string{"speculative"}));
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b)); // publish opened no connection
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: a single slot's channel drop re-dials only that slot; another slot is "
          "untouched (per-slot isolation)",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        // A three-node-aware net: node A reaches both B and a second peer C (C is
        // node B's transport too — a second endpoint B listens on is unnecessary; we
        // assert single-slot isolation, so two independent slots on A suffice).
        inproc_bus<manual_clock>      bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t                   transport_a{ex, bus};
        transport_t                   transport_b{ex, bus};
        transport_t                   transport_c{ex, bus};

        plexus::log::null_logger sink;
        engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false);
        engine b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false);
        engine c(transport_c, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, sink, false);

        plexus::node_id id_b = make_id(0xB2);
        plexus::node_id id_c = make_id(0xC3);
        endpoint        ep_b{"inproc", "node-b"};
        endpoint        ep_c{"inproc", "node-c"};
        a.listen({"inproc", "node-a"});
        b.listen(ep_b);
        c.listen(ep_c);

        a.note_peer(id_b, ep_b);
        a.note_peer(id_c, ep_c);
        a.reach(id_b);
        a.reach(id_c);
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        REQUIRE(a.is_connected(id_c));

        const auto b_before = a.attempt_count(id_b);
        const auto c_before = a.attempt_count(id_c);
        const auto b_epoch  = a.session_for(id_b)->session_id();

        // Drop ONLY slot B's channel: its driver re-dials. Slot C is untouched.
        a.registry().driver_for(id_b).on_channel_dropped();
        REQUIRE(a.attempt_count(id_b) == b_before + 1); // B's slot advanced
        REQUIRE(a.attempt_count(id_c) == c_before);     // C's slot did NOT (no set-wide loop)

        // Drive the backoff: B re-dials and re-handshakes a FRESH epoch.
        manual_clock::advance(std::chrono::milliseconds(10001));
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        REQUIRE(a.session_for(id_b)->session_id() != b_epoch);
        REQUIRE(a.is_connected(id_c)); // C never disturbed
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
