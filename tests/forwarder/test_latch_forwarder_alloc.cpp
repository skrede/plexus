#include "test_latch_forwarder_common.h"

#include "support/alloc_counter.h"

using namespace latch_forwarder_fixture;

namespace {

// The non-allocating sink Policy used by the LATCH-NOALLOC gate: a byte_channel that records
// each send's size without copying, so a forwarder<sink_policy> latched publish exercises
// framing + retention with no transport-side allocation.
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

TEST_CASE("latched retention adds no allocation beyond the frame-once publish", "[latch][forwarder]")
{
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    const std::string payload = "steady-state-body";
    constexpr int K           = 256;

    // The per-publish allocation count over a single subscriber, topic either latched or not. The
    // retain holds the already-framed shared owner by addref, so it adds nothing beyond it.
    const auto allocs_per_publish = [&](bool latched)
    {
        sink_executor ex;
        sink_channel ch(ex);
        plexus::log::null_logger log_sink;
        sink_forwarder fwd{log_sink};
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

    // Retention adds nothing beyond the publish: the latched and unlatched per-publish counts
    // are identical — the slot retains the frame owner by addref.
    REQUIRE(allocs_per_publish(true) == allocs_per_publish(false));
}
