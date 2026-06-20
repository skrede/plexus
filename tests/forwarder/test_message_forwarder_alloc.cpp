#include "test_message_forwarder_common.h"

#include "support/alloc_counter.h"

using namespace message_forwarder_fixture;

namespace {

// A non-allocating sink Policy used solely by the forwarder-level alloc gate.
// Its byte_channel records each send's size into a counter without copying the
// bytes, so a forwarder<sink_policy> publish exercises framing + the fan-out
// dispatch loop with no transport-side allocation — isolating the forwarder's
// own heap behavior. The timer/executor are inert: the alloc gate never steps.
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

TEST_CASE("frame-once fan-out: the per-publish allocation does not scale with the subscriber count",
          "[forwarder]")
{
    // Measured over the non-allocating sink Policy so the only heap traffic in the loop
    // is the forwarder's own. The owner-carry path frames ONCE per publish into a single
    // shared owning buffer and addref-shares it to every subscriber (frame-once-fan-to-N),
    // so the per-publish heap cost is the ONE shared frame owner — independent of how many
    // destinations it fans to. (The inproc bus's per-packet copy is the transport's
    // allocation and is audited separately.) The sink channel has no backpressure signal,
    // so the egress short-circuits to a direct owner-converting send — no band, no copy.
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    const std::string payload = "steady-state-body";
    constexpr int     K       = 256;

    // The per-publish allocation count for a fan-out of N subscribers, after warm-up.
    const auto allocs_per_publish = [&](int subscribers)
    {
        sink_executor                              ex;
        std::vector<std::unique_ptr<sink_channel>> chs;
        sink_forwarder                             fwd{};
        for(int i = 0; i < subscribers; ++i)
        {
            chs.emplace_back(std::make_unique<sink_channel>(ex));
            fwd.attach(sink_forwarder::peer{*chs.back(), "node-" + std::to_string(i)}, "alpha");
        }
        fwd.publish("alpha", as_bytes(payload)); // warm-up
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish("alpha", as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        for(const auto &c : chs)
            REQUIRE(c->sends >= static_cast<std::size_t>(K)); // every publish fanned to each
        return (after - before) / K;
    };

    // frame-once-fan-to-N: the same per-publish alloc count whether fanning to 2 or to 8 —
    // the cost is the single shared frame owner, NOT one buffer per destination.
    const auto two   = allocs_per_publish(2);
    const auto eight = allocs_per_publish(8);
    REQUIRE(two == eight);
}
