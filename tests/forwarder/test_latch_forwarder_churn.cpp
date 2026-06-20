#include "test_latch_forwarder_common.h"

#include "support/alloc_counter.h"

using namespace latch_forwarder_fixture;

namespace {

// The non-allocating sink Policy used by the LATCH-NOALLOC gate (mirrors the
// message-forwarder oracle's sink Policy): a byte_channel that records each send's
// size without copying, so a forwarder<sink_policy> latched publish exercises
// framing + retention with no transport-side allocation.
struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d)
    {
        total_bytes += d.size();
        ++sends;
    }
    void                 close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &) {}
    sink_timer(sink_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

TEST_CASE("latch replay targets only the new subscriber, no double-receive", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch_a(ex), ch_b(ex);
    capture           cap_a(ex), cap_b(ex);
    auto              peer_a = make_peer(ch_a, cap_a, "node-a");
    auto              peer_b = make_peer(ch_b, cap_b, "node-b");

    forwarder fwd{};
    fwd.latch("topic");
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic")); // A before the publish
    ex.drain();
    cap_a.frames.clear();

    fwd.publish("topic", as_bytes(std::string{"the-value"})); // A gets it live, once
    ex.drain();

    // B explicitly requests single-newest replay; it is the late joiner that must
    // receive the retained value, so it declares the durability that asks for it.
    plexus::io::subscriber_qos sub_qos_b;
    sub_qos_b.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer_b, "topic", std::nullopt,
                                  sub_qos_b)); // B after — gets the replay
    ex.drain();

    REQUIRE(data_bodies(cap_a).size() == 1); // A: exactly the one live frame, no replay
    auto bodies_b = data_bodies(cap_b);
    REQUIRE(bodies_b.size() == 1);
    REQUIRE(bodies_b[0] == "the-value");
}

TEST_CASE("latch retention survives subscriber churn", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch_a(ex);
    capture           cap_a(ex);
    auto              peer_a = make_peer(ch_a, cap_a, "node-a");

    forwarder fwd{};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"survivor"}));
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic"));
    ex.drain();
    fwd.detach_all(peer_a); // the last subscriber leaves

    inproc_channel<> ch_b(ex);
    capture          cap_b(ex);
    auto             peer_b = make_peer(ch_b, cap_b, "node-b");
    // The new late joiner explicitly requests single-newest replay; the retained value
    // is delivered only to a subscriber that declares the durability that asks for it.
    plexus::io::subscriber_qos sub_qos_b;
    sub_qos_b.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer_b, "topic", std::nullopt, sub_qos_b)); // a new late joiner
    ex.drain();

    auto bodies = data_bodies(cap_b);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "survivor"); // the value outlived the churn
}

TEST_CASE("multi-publisher to one latched topic is last-writer-wins", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"writer-1"}));
    fwd.publish("topic", as_bytes(std::string{"writer-2"}));
    ex.drain();

    // The subscriber explicitly requests single-newest replay; latch retention is
    // delivered only to a subscriber that declares the durability that asks for it.
    plexus::io::subscriber_qos sub_qos;
    sub_qos.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, sub_qos));
    ex.drain();
    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "writer-2"); // one slot per topic_hash, last write retained
}

TEST_CASE("latched retention adds no allocation beyond the frame-once publish",
          "[latch][forwarder]")
{
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    const std::string payload = "steady-state-body";
    constexpr int     K       = 256;

    // The per-publish allocation count over a single subscriber, with the topic either
    // latched (retain in the loop) or not (publish + fan-out only). The retain holds the
    // already-framed shared owner by addref, so it adds nothing beyond the publish owner.
    const auto allocs_per_publish = [&](bool latched)
    {
        sink_executor        ex;
        sink_channel         ch(ex);
        sink_forwarder       fwd{};
        sink_forwarder::peer peer{ch, "node-a"};
        if(latched)
            fwd.latch("topic");
        fwd.attach_for_fanout(peer, "topic");
        fwd.publish("topic", as_bytes(payload)); // warm-up: grow scratch AND first-touch the slot
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish("topic", as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        REQUIRE(ch.sends >= static_cast<std::size_t>(K)); // every latched publish fanned out
        return after - before;
    };

    // Retention adds nothing beyond the publish: the latched and unlatched per-publish
    // allocation counts are identical — the slot retains the frame owner by addref.
    REQUIRE(allocs_per_publish(true) == allocs_per_publish(false));
}
