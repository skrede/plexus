#include "test_latch_replay_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace latch_replay_fixture;

TEST_CASE("inproc latch replay delivers the late joiner the retained value through frame_router, "
          "looped",
          "[integration][latch][inproc]")
{
    // Deterministic by construction (virtual clock + drain): a fresh
    // bus/executor/forwarder per iteration surfaces any flake as a mismatch on some
    // iteration rather than a one-off pass. The subscriber routes the replayed frame
    // through a real frame_router::on_unidirectional, so the assertion proves the
    // retained bytes survive the production demux byte-identical, not a hand-strip.
    constexpr int     k_iterations = 100;
    const std::string payload      = "retained-opaque-value";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        receive_sink      sink(ex);
        auto              peer = make_peer(ch, sink, "late-node");

        plexus::log::null_logger log_sink;
        forwarder                fwd{log_sink};
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(payload)); // retained with ZERO subscribers
        ex.drain();
        REQUIRE(sink.bodies.empty()); // nothing fanned out before the late join

        // The late joiner explicitly requests single-newest replay; latch retention is
        // delivered only to a subscriber that declares the durability that asks for it.
        plexus::io::subscriber_qos sub_qos;
        sub_qos.durability_mode = plexus::io::durability::latest;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt,
                                      sub_qos)); // the late join drives the replay
        ex.drain();

        REQUIRE(sink.bodies.size() == 1);
        REQUIRE(sink.bodies[0] == payload); // the EXACT retained bytes, every iteration
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("inproc non-latched topic does not replay on a late subscribe, looped", "[integration][latch][inproc]")
{
    // The non-latched NEGATIVE (guards against an accidental always-replay): the
    // same late-join setup with NO latch records NOTHING through the router until a
    // live publish arrives. Deterministic over inproc — this is the authoritative
    // negative guard (no grace window needed; the drain is exhaustive).
    constexpr int k_iterations = 100;
    int           held         = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        receive_sink      sink(ex);
        auto              peer = make_peer(ch, sink, "late-node");

        plexus::log::null_logger log_sink;
        forwarder                fwd{log_sink};
        fwd.publish("topic", as_bytes(std::string{"live-only"})); // not latched, no subscriber
        ex.drain();

        REQUIRE(fwd.attach_for_fanout(peer, "topic")); // late join: NO replay
        ex.drain();
        REQUIRE(sink.bodies.empty()); // nothing replayed

        fwd.publish("topic", as_bytes(std::string{"first-live"})); // now a live publish arrives
        ex.drain();
        REQUIRE(sink.bodies.size() == 1);
        REQUIRE(sink.bodies[0] == "first-live");
        ++held;
    }
    REQUIRE(held == k_iterations);
}

// A non-allocating sink Policy for the LATCH-NOALLOC gate (mirrors the message
// forwarder oracle's sink Policy): the byte_channel records send sizes without
// copying, so a forwarder<sink_policy> latched publish exercises framing +
// retention with no transport-side allocation — isolating the retention path so a
// clean 0 is the proof.
namespace {

struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &)
    {
    }
    sink_channel(sink_executor &, std::error_code &)
    {
    }

    void send(std::span<const std::byte> d)
    {
        total_bytes += d.size();
        ++sends;
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &)
    {
    }
    sink_timer(sink_executor &, std::error_code &)
    {
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }
    void cancel()
    {
    }
};

struct sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::Policy<sink_policy>);

}
