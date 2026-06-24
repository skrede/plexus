#include "test_message_forwarder_common.h"

using namespace message_forwarder_fixture;

TEST_CASE("detach on 1->0 emits exactly one unsubscribe_request", "[forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    fwd.attach(peer, "alpha");
    fwd.attach(peer, "alpha"); // refcount 2
    ex.drain();
    cap.frames.clear();

    REQUIRE_FALSE(fwd.detach(peer, "alpha")); // 2->1, no emit
    REQUIRE(fwd.detach(peer, "alpha"));       // 1->0, emit
    ex.drain();
    REQUIRE(count_unsubscribes(cap) == 1);
}

TEST_CASE("receive tail resolves the fqn by topic_hash and hands exact bytes up", "[forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    fwd.attach(peer, "alpha"); // registers the topic_hash -> fqn resolution

    const std::string body  = "the-opaque-payload";
    auto              frame = make_data_frame("alpha", body);

    // The frame_router owns the header strip + type switch; its unidirectional
    // consumer hands the inner payload (header-off) to the realigned deliver().
    std::string              got_fqn;
    std::string              got_body;
    plexus::io::frame_router router{sink};
    router.on_unidirectional(
            [&](const plexus::wire::frame_header &, std::span<const std::byte> inner)
            {
                fwd.deliver(peer, inner, plexus::node_id{}, /*has_source_identity=*/false,
                            [&](std::string_view fqn, std::span<const std::byte> data)
                            {
                                got_fqn.assign(fqn);
                                got_body.assign(reinterpret_cast<const char *>(data.data()), data.size());
                            });
            });
    router.route(frame);

    REQUIRE(got_fqn == "alpha");
    REQUIRE(got_body == body);
}

TEST_CASE("no-subscriber publish sends nothing (demand-driven)", "[forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    fwd.publish("alpha", as_bytes(std::string{"nobody-home"}));
    ex.drain();
    REQUIRE_FALSE(bus.has_pending_packets()); // nothing was ever enqueued
}

TEST_CASE("receive tail warn-and-drops a malformed frame through the injected logger", "[forwarder]")
{
    counting_logger   log;
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{log, plexus::io::global_default_max_message_bytes};
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    // An inner payload too short to be a unidirectional header (< 25 bytes), so
    // the realigned deliver() — which now receives the header-off inner payload
    // (the router owns the header strip) — fails decode_unidirectional and drops.
    std::vector<std::byte> garbage(8, std::byte{0xAB});

    bool fired = false;
    fwd.deliver(peer, garbage, plexus::node_id{}, /*has_source_identity=*/false, [&](std::string_view, std::span<const std::byte>) { fired = true; });

    REQUIRE_FALSE(fired);    // dropped: no subscriber callback
    REQUIRE(log.count == 1); // the warn seam fired exactly once
}

TEST_CASE("default forwarder drops a malformed frame silently via null_logger", "[forwarder]")
{
    inproc_bus<>             bus;
    inproc_executor<>        ex(bus);
    plexus::log::null_logger sink; // an inert sink: warn-and-drop stays silent
    forwarder                fwd{sink};
    inproc_channel<>         ch(ex);
    capture                  cap(ex);
    auto                     peer = make_peer(ch, cap, "node-a");

    std::vector<std::byte> garbage(8, std::byte{0xAB});

    bool fired = false;
    REQUIRE_NOTHROW(fwd.deliver(peer, garbage, plexus::node_id{}, /*has_source_identity=*/false, [&](std::string_view, std::span<const std::byte>) { fired = true; }));
    REQUIRE_FALSE(fired); // dropped silently, no crash
}
