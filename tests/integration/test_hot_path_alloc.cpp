#include "plexus/io/message_forwarder.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <system_error>

using namespace plexus;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A non-allocating sink Policy: its byte_channel records each send's size and
// count without copying the bytes, so a forwarder<sink_policy> publish exercises
// the FULL steady-state path the architecture invariant governs — frame ONCE
// into reused scratch + the fan-out dispatch loop — with no transport-side
// allocation masking the forwarder's own heap behavior. The executor/timer are
// inert; the audit never steps them.
struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d) { total_bytes += d.size(); ++sends; }
    void close() {}
    [[nodiscard]] io::endpoint remote_endpoint() const { return {}; }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(detail::move_only_function<void()>) {}
    void on_error(detail::move_only_function<void(io::io_error)>) {}
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>) {}

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &) {}
    sink_timer(sink_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct sink_policy
{
    using executor_type = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type = sink_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

// SLICE-3 determinism invariant: the steady-state publish -> frame-once -> fan-out
// loop allocates ZERO bytes after warm-up. The forwarder frames each publish once
// into reused member scratch and shares the one buffer across N subscribers; the
// architecture mandates no allocation on this hot path ("allocate at setup;
// deterministic message loop"). Measured over the sink Policy so the global
// new/delete delta reflects the forwarder's own heap behavior — framing into the
// reused scratch + the dispatch loop — across K publishes to N subscribers.
TEST_CASE("steady-state publish->frame-once->fan-out loop is zero-alloc", "[integration]")
{
    using forwarder = io::message_forwarder<sink_policy>;

    constexpr int N = 8;       // fan-out width
    constexpr int K = 1024;    // steady-state message count
    const std::string fqn = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    std::vector<std::unique_ptr<sink_channel>> channels;
    std::vector<forwarder::peer> peers;
    channels.reserve(N);
    peers.reserve(N);

    forwarder fwd;
    for(int i = 0; i < N; ++i)
    {
        channels.push_back(std::make_unique<sink_channel>(ex));
        peers.push_back(forwarder::peer{*channels.back(), "node-" + std::to_string(i)});
        fwd.attach(peers.back(), fqn);
    }

    // Warm-up: one publish grows the two reused scratch buffers to steady size.
    fwd.publish(fqn, as_bytes(payload));

    std::size_t sends_before = 0;
    for(const auto &ch : channels)
        sends_before += ch->sends;

    // Audit: K publishes through the frame-once fan-out, global new/delete delta.
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.publish(fqn, as_bytes(payload));
    const auto after = plexus::testing::alloc_count();

    std::size_t sends_after = 0;
    for(const auto &ch : channels)
        sends_after += ch->sends;

    REQUIRE(sends_after - sends_before == static_cast<std::size_t>(K) * N); // every publish fanned to all N
    REQUIRE(after - before == 0); // zero allocation across the steady-state loop
}
