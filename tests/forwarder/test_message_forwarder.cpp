#include "test_message_forwarder_common.h"

using namespace message_forwarder_fixture;

TEST_CASE("attach refcount gate emits exactly one subscribe on 0->1", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        REQUIRE(fwd.attach(peer, "alpha"));       // 0->1
        REQUIRE_FALSE(fwd.attach(peer, "alpha")); // 1->2, no emit
        ex.drain();

        REQUIRE(count_subscribes(cap) == 1);
    }
}

TEST_CASE("resurrection re-emits one subscribe per peer-rooted remembered topic", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        fwd.remember_demand("node-a", "alpha");
        fwd.remember_demand("node-a", "beta");
        ex.drain();
        cap.frames.clear();

        resurrect(fwd, peer);
        ex.drain();
        REQUIRE(count_subscribes(cap) == 2); // one per recorded remembered topic
    }
}

TEST_CASE("resurrection carries the remembered type_id onto the re-subscribe", "[forwarder][type_id]")
{
    // The reconnect type-gate drop: a demand remembered WITH a type_id must re-emit a
    // subscribe carrying that SAME type_id when the counted path resurrects it. Looped
    // over several remember/resurrect cycles (no success from a single run).
    constexpr std::uint64_t k_type_id = 0xC0FFEE1234567890ULL;
    constexpr int           k_cycles  = 16;
    int                     proven    = 0;
    for(int iter = 0; iter < k_cycles; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        fwd.remember_demand("node-a", "alpha", plexus::io::subscriber_qos{}, k_type_id);
        resurrect(fwd, peer);
        ex.drain();

        const auto type_hash = first_subscribe_type_hash(cap);
        REQUIRE(type_hash.has_value());
        REQUIRE(*type_hash == k_type_id); // the gate survived the resurrection
        ++proven;
    }
    REQUIRE(proven == k_cycles);
}

TEST_CASE("resurrection keeps an undeclared demand untyped on the re-subscribe", "[forwarder][type_id]")
{
    // The absence-is-distinct mirror: a demand remembered WITHOUT a type_id re-emits the
    // undeclared sentinel (0), never a fabricated type.
    for(int iter = 0; iter < 16; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        fwd.remember_demand("node-a", "alpha");
        resurrect(fwd, peer);
        ex.drain();

        const auto type_hash = first_subscribe_type_hash(cap);
        REQUIRE(type_hash.has_value());
        REQUIRE(*type_hash == 0);
    }
}

TEST_CASE("frame-once fan-out delivers byte-identical frames to each subscriber", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch_a(ex), ch_b(ex);
        capture           cap_a(ex), cap_b(ex);
        auto              peer_a = make_peer(ch_a, cap_a, "node-a");
        auto              peer_b = make_peer(ch_b, cap_b, "node-b");

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        fwd.attach(peer_a, "alpha");
        fwd.attach(peer_b, "alpha");
        ex.drain();
        cap_a.frames.clear();
        cap_b.frames.clear();

        fwd.publish("alpha", as_bytes(std::string{"payload-bytes"}));
        ex.drain();

        REQUIRE(cap_a.frames.size() == 1);
        REQUIRE(cap_b.frames.size() == 1);
        // Byte-identical, not merely equal-length: no per-peer session stamp.
        REQUIRE(cap_a.frames[0] == cap_b.frames[0]);
    }
}

TEST_CASE("detach_all stops fan-out", "[forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    fwd.attach(peer, "alpha");
    ex.drain();
    cap.frames.clear();

    fwd.detach_all(peer);
    fwd.publish("alpha", as_bytes(std::string{"after-detach"}));
    ex.drain();
    REQUIRE(cap.frames.empty()); // no subscriber remains
}
