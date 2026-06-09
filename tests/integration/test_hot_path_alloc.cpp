#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>
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

// The message_info delivery path is ALSO zero-alloc after warm-up: the 3-arg deliver
// resolves the fqn by topic_hash and hands a STACK message_info to the callback. The
// info is a POD assembled on the stack (no heap), the callback is captured ONCE outside
// the loop (no per-frame callable allocation), and decode_unidirectional only
// subspans the borrowed inner buffer. So the steady-state receive path must allocate
// zero — the same determinism invariant the publish path obeys.
TEST_CASE("steady-state message_info deliver path is zero-alloc", "[integration]")
{
    using forwarder = io::message_forwarder<sink_policy>;

    constexpr int K = 1024;
    const std::string fqn = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    sink_channel ch(ex);
    forwarder::peer peer{ch, "node-rx"};

    forwarder fwd;
    fwd.attach(peer, fqn);   // resolves topic_hash -> fqn for the receive tail

    // Build the inner unidirectional payload ONCE (the borrowed receive buffer).
    wire::unidirectional_header uhdr{
        .source     = wire::endpoint_source_type::publisher,
        .sequence   = 1,
        .topic_hash = wire::fqn_topic_hash(fqn)};
    auto inner = wire::encode_unidirectional(uhdr, as_bytes(payload));

    // The session-assembled metadata half: a stack POD reused every iteration.
    io::message_info info{};
    info.source_timestamp   = 1000;
    info.from_intra_process = false;

    std::size_t seen = 0;
    auto on_message = [&](std::string_view, std::span<const std::byte>, const io::message_info &) { ++seen; };

    // Warm-up: one deliver exercises any first-touch growth in the resolution path.
    fwd.deliver(peer, inner, info, on_message);

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.deliver(peer, inner, info, on_message);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(seen == static_cast<std::size_t>(K) + 1);   // every deliver reached the callback
    REQUIRE(after - before == 0);                       // zero allocation across the steady-state loop
}

#ifdef PLEXUS_HAVE_ASIO_MUX
#include "plexus/io/polymorphic_byte_channel.h"

// The same steady-state no-alloc invariant must hold when the publish loop fans over a
// type-erased polymorphic_byte_channel instead of a concrete channel: the erasure is ONE virtual hop
// per send to the owned concrete channel, minted ONCE at wrap (here at setup), and the
// adapter stores no per-verb callable — so the abstract base adds zero steady-state heap
// blocks. Measured directly over a vector of polymorphic_byte_channels wrapping the inert sink_channel
// (no forwarder Policy is needed: the gate is the channel layer's per-send behavior).
TEST_CASE("steady-state fan-out over a type-erased polymorphic_byte_channel is zero-alloc", "[integration]")
{
    namespace pio = plexus::io;

    constexpr int N = 8;
    constexpr int K = 1024;
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    std::vector<std::unique_ptr<pio::polymorphic_byte_channel>> channels;
    std::vector<sink_channel *> sinks;   // observers into the owned concrete channels
    channels.reserve(N);
    sinks.reserve(N);
    for(int i = 0; i < N; ++i)
    {
        auto inner = std::make_unique<sink_channel>(ex);
        sinks.push_back(inner.get());
        channels.push_back(std::make_unique<pio::polymorphic_byte_channel>(
            std::make_unique<pio::channel_adapter<sink_channel>>(std::move(inner))));
    }

    // Warm-up: one fan-out round before measuring (no scratch grows here, but mirror the gate).
    for(auto &ch : channels)
        ch->send(as_bytes(payload));

    std::size_t sends_before = 0;
    for(const auto *s : sinks)
        sends_before += s->sends;

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        for(auto &ch : channels)
            ch->send(as_bytes(payload));
    const auto after = plexus::testing::alloc_count();

    std::size_t sends_after = 0;
    for(const auto *s : sinks)
        sends_after += s->sends;

    REQUIRE(sends_after - sends_before == static_cast<std::size_t>(K) * N);
    REQUIRE(after - before == 0); // the abstract base adds zero steady-state allocation
}
#endif
