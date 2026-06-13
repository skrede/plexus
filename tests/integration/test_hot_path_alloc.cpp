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

// A sink channel that DOES expose backpressured() so a forwarder<banding_sink_policy>
// publish routes through the egress scheduler's band->drain path (not the no-backpressured
// short-circuit). The test holds the reported occupancy at 0 so accepts() is always true
// and the synchronous drain sends every banded frame immediately — letting the no-alloc
// gate measure the scheduler enqueue->band->pop->send path. It also satisfies the erasure
// (channel_adapter<C>::backpressured forwards to it), so it doubles as the erased-channel
// member in the type-erasure no-alloc gate.
struct banding_sink_channel
{
    explicit banding_sink_channel(sink_executor &) {}
    banding_sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d) { total_bytes += d.size(); ++sends; }
    void close() {}
    [[nodiscard]] io::endpoint remote_endpoint() const { return {}; }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(detail::move_only_function<void()>) {}
    void on_error(detail::move_only_function<void(io::io_error)>) {}
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>) {}
    [[nodiscard]] std::size_t backpressured() const noexcept { return 0; }
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return 1; }

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct banding_sink_policy
{
    using executor_type = sink_executor &;
    using byte_channel_type = banding_sink_channel;
    using timer_type = sink_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<banding_sink_policy>);

}

// Determinism invariant: the steady-state publish -> frame-once -> fan-out loop frames
// each publish ONCE into a single shared owning buffer and addref-shares that one owner
// to every subscriber (frame-once-fan-to-N), so the per-publish heap cost is the ONE
// shared frame owner — independent of how many destinations it fans to. Measured over
// the sink Policy so the global new/delete delta reflects the forwarder's own heap
// behavior. (The absolute per-publish owner allocation is the producer-ownership cost a
// recycled loan removes later.)
TEST_CASE("steady-state publish->frame-once->fan-out loop stays frame-once: per-publish allocation does not scale with the subscriber count", "[integration]")
{
    using forwarder = io::message_forwarder<sink_policy>;

    constexpr int K = 1024;    // steady-state message count
    const std::string fqn = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count for a fan-out of N subscribers, after warm-up.
    const auto allocs_per_publish = [&](int subscribers) {
        sink_executor ex;
        std::vector<std::unique_ptr<sink_channel>> channels;
        std::vector<forwarder::peer> peers;
        forwarder fwd{};
        for(int i = 0; i < subscribers; ++i)
        {
            channels.push_back(std::make_unique<sink_channel>(ex));
            peers.push_back(forwarder::peer{*channels.back(), "node-" + std::to_string(i)});
            fwd.attach(peers.back(), fqn);
        }
        fwd.publish(fqn, as_bytes(payload));   // warm-up: reach the steady owner-buffer size
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish(fqn, as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        std::size_t sends = 0;
        for(const auto &ch : channels)
            sends += ch->sends;
        REQUIRE(sends >= static_cast<std::size_t>(K) * subscribers);   // every publish fanned to all
        return (after - before) / K;
    };

    // frame-once-fan-to-N: the same per-publish alloc count whether fanning to 2 or to 8 —
    // the cost is the single shared frame owner, NOT one buffer per destination.
    REQUIRE(allocs_per_publish(2) == allocs_per_publish(8));
}

// The KEEP_LAST-N history-ring retain path adds NOTHING beyond the frame-once publish
// owner: a latched topic of depth N pushes the already-framed shared owner into a ring
// of N slots that retain it by addref (the slots reuse their handles in steady state).
// Once every slot has been touched once (the warm-up publishes at least N frames), the
// retain adds zero heap — so the latched-minus-unlatched per-publish allocation DELTA
// is zero. (The publish itself still allocates its one frame owner; that owner cost is
// common to both arms and cancels in the delta.)
TEST_CASE("steady-state depth-N history-ring retain adds no allocation beyond the frame-once publish", "[integration]")
{
    using forwarder = io::message_forwarder<sink_policy>;

    constexpr std::uint32_t N = 8;   // ring depth
    constexpr int K = 1024;          // steady-state message count
    const std::string fqn = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count over a single subscriber, with the topic either
    // latched (depth-N retain in the loop) or not (publish + fan-out only).
    const auto allocs_per_publish = [&](bool latched) {
        sink_executor ex;
        sink_channel ch(ex);
        forwarder::peer peer{ch, "node-a"};
        forwarder fwd{};
        if(latched)
            fwd.declare(fqn, topic_qos{.latch = true, .depth = N});
        fwd.attach(peer, fqn);
        // Warm-up: publish N times so EVERY ring slot's handle is first-touched.
        for(std::uint32_t i = 0; i < N; ++i)
            fwd.publish(fqn, as_bytes(payload));
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish(fqn, as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        return after - before;
    };

    // The depth-N retain adds nothing beyond the publish: the latched and unlatched
    // per-publish allocation counts are identical — the ring retains the owner by addref.
    REQUIRE(allocs_per_publish(true) == allocs_per_publish(false));
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

    forwarder fwd{};
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

    // The session peer's node_id (the gid's node_id half on reconstruction).
    node_id src{};
    src[15] = std::byte{0x5A};

    // --- bytes-only path (gid flag clear) ---
    // Warm-up: one deliver exercises any first-touch growth in the resolution path.
    fwd.deliver(peer, inner, info, src, /*has_source_identity=*/false, on_message);

    plexus::testing::reset_alloc_count();
    auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.deliver(peer, inner, info, src, /*has_source_identity=*/false, on_message);
    auto after = plexus::testing::alloc_count();

    REQUIRE(seen == static_cast<std::size_t>(K) + 1);   // every deliver reached the callback
    REQUIRE(after - before == 0);                       // zero allocation across the steady-state loop

    // --- source-identity path (gid flag set) ---
    // The inner now carries a varint endpoint counter; deliver decodes it and constructs
    // the publisher_gid IN-PLACE into the stack message_info — the reconstruction must add
    // zero steady-state heap, same as the bytes-only path.
    auto inner_gid = wire::encode_unidirectional(uhdr, as_bytes(payload), std::uint64_t{0x1234});
    std::size_t gid_seen = 0;
    bool gid_ok = true;
    auto on_gid = [&](std::string_view, std::span<const std::byte>, const io::message_info &mi) {
        ++gid_seen;
        if(!mi.source_identity || mi.source_identity->endpoint_counter() != 0x1234)
            gid_ok = false;
    };
    fwd.deliver(peer, inner_gid, info, src, /*has_source_identity=*/true, on_gid);   // warm-up

    plexus::testing::reset_alloc_count();
    before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.deliver(peer, inner_gid, info, src, /*has_source_identity=*/true, on_gid);
    after = plexus::testing::alloc_count();

    REQUIRE(gid_seen == static_cast<std::size_t>(K) + 1);
    REQUIRE(gid_ok);                  // every delivery reconstructed the gid
    REQUIRE(after - before == 0);     // gid reconstruction is zero steady-state heap
}

// The egress-scheduler band->drain path adds NOTHING beyond the frame-once publish owner.
// Over a channel that exposes backpressured() (held at 0 so the destination always
// accepts), publish routes through enqueue -> band -> pop_highest -> send rather than the
// no-backpressured short-circuit; the band slot HOLDS the shared frame owner by addref
// and hands it to the send on drain (no per-destination band copy). So the per-publish
// heap cost is the single shared owner — independent of how many destinations the band
// fans to. Each enqueue/immediate-drain advances the band's FIFO ring by one slot, so the
// warm-up cycles the in-use band's whole pooled ring once (k_band_depth publishes) to
// first-touch every slot's handle.
TEST_CASE("steady-state publish through the egress scheduler bands stays frame-once: per-publish allocation does not scale with the subscriber count", "[integration]")
{
    using forwarder = io::message_forwarder<banding_sink_policy>;

    constexpr int K = 1024;                                       // steady-state message count
    const std::size_t warm = io::detail::k_band_depth;            // cycle the in-use band ring once
    const std::string fqn = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count for a fan-out of N subscribers through the bands.
    const auto allocs_per_publish = [&](int subscribers) {
        sink_executor ex;
        std::vector<std::unique_ptr<banding_sink_channel>> channels;
        std::vector<forwarder::peer> peers;
        forwarder fwd{};
        for(int i = 0; i < subscribers; ++i)
        {
            channels.push_back(std::make_unique<banding_sink_channel>(ex));
            peers.push_back(forwarder::peer{*channels.back(), "node-" + std::to_string(i)});
            fwd.attach(peers.back(), fqn);
        }
        // Warm-up: cycle the in-use band's full pooled ring across all destinations so
        // every band slot's handle is first-touched, plus the scratch + band map nodes.
        for(std::size_t i = 0; i < warm; ++i)
            fwd.publish(fqn, as_bytes(payload));
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish(fqn, as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        std::size_t sends = 0;
        for(const auto &ch : channels)
            sends += ch->sends;
        REQUIRE(sends >= static_cast<std::size_t>(K) * subscribers);   // every publish drained to all
        return (after - before) / K;
    };

    // frame-once-fan-to-N through the bands: the same per-publish alloc count whether
    // fanning to 2 or to 8 — the band slots hold the single shared owner by addref, NOT
    // one buffer per destination.
    REQUIRE(allocs_per_publish(2) == allocs_per_publish(8));
}

#ifdef PLEXUS_HAVE_ASIO_MUX
#include "plexus/io/polymorphic_byte_channel.h"

// The same steady-state no-alloc invariant must hold when the publish loop fans over a
// type-erased polymorphic_byte_channel instead of a concrete channel: the erasure is ONE virtual hop
// per send to the owned concrete channel, minted ONCE at wrap (here at setup), and the
// adapter stores no per-verb callable — so the abstract base adds zero steady-state heap
// blocks. Measured directly over a vector of polymorphic_byte_channels wrapping the inert
// banding_sink_channel (it exposes the backpressured() read the erasure now forwards; no
// forwarder Policy is needed: the gate is the channel layer's per-send behavior).
TEST_CASE("steady-state fan-out over a type-erased polymorphic_byte_channel is zero-alloc", "[integration]")
{
    namespace pio = plexus::io;

    constexpr int N = 8;
    constexpr int K = 1024;
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    std::vector<std::unique_ptr<pio::polymorphic_byte_channel>> channels;
    std::vector<banding_sink_channel *> sinks;   // observers into the owned concrete channels
    channels.reserve(N);
    sinks.reserve(N);
    for(int i = 0; i < N; ++i)
    {
        auto inner = std::make_unique<banding_sink_channel>(ex);
        sinks.push_back(inner.get());
        channels.push_back(std::make_unique<pio::polymorphic_byte_channel>(
            std::make_unique<pio::channel_adapter<banding_sink_channel>>(std::move(inner))));
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
