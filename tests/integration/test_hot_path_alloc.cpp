#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/peer_liveliness.h"
#include "plexus/detail/compat.h"

#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"

#include "support/alloc_counter.h"

// The asio/crypto backend headers pull in <asio/...> whose own code references the
// UNQUALIFIED name `asio`; they MUST precede `using namespace plexus;` below, because that
// using-directive makes plexus::asio visible and an unqualified `asio` then resolves
// ambiguously between ::asio and plexus::asio inside the asio headers.
#ifdef PLEXUS_HAVE_ASIO_MUX
    #include "plexus/asio/udp_server.h"
    #include "plexus/datagram/detail/send_queue.h"

    #include <asio/io_context.hpp>
    #include <asio/ip/udp.hpp>
#endif

#ifdef PLEXUS_HAVE_CRYPTO_DATAGRAM
    #include "plexus/crypto/datagram_authenticated_channel.h"
    #include "plexus/crypto/key_schedule.h"
    #include "plexus/crypto/aead_epoch.h"
    #include "plexus/crypto/aead_cipher.h"

    #include "plexus/wire/frame_codec.h"
    #include "plexus/wire/frame.h"
#endif

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
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
    io::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(detail::move_only_function<void()>)
    {
    }
    void on_error(detail::move_only_function<void(io::io_error)>)
    {
    }
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>)
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
    void async_wait(detail::move_only_function<void(std::error_code)>)
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

    static void post(executor_type, detail::move_only_function<void()> fn)
    {
        fn();
    }
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
    explicit banding_sink_channel(sink_executor &)
    {
    }
    banding_sink_channel(sink_executor &, std::error_code &)
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
    io::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(detail::move_only_function<void()>)
    {
    }
    void on_error(detail::move_only_function<void(io::io_error)>)
    {
    }
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>)
    {
    }
    std::size_t backpressured() const noexcept
    {
        return 0;
    }
    std::uint64_t scheduler_key() const noexcept
    {
        return 1;
    }

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct banding_sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = banding_sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, detail::move_only_function<void()> fn)
    {
        fn();
    }
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
TEST_CASE("steady-state publish->frame-once->fan-out loop stays frame-once: per-publish allocation "
          "does not scale with the subscriber count",
          "[integration]")
{
    using forwarder = io::message_forwarder<sink_policy>;

    constexpr int K           = 1024; // steady-state message count
    const std::string fqn     = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count for a fan-out of N subscribers, after warm-up.
    const auto allocs_per_publish = [&](int subscribers)
    {
        sink_executor ex;
        std::vector<std::unique_ptr<sink_channel>> channels;
        std::vector<forwarder::peer> peers;
        plexus::log::null_logger log_sink;
        forwarder fwd{log_sink};
        for(int i = 0; i < subscribers; ++i)
        {
            channels.push_back(std::make_unique<sink_channel>(ex));
            peers.push_back(forwarder::peer{*channels.back(), "node-" + std::to_string(i)});
            fwd.attach(peers.back(), fqn);
        }
        fwd.publish(fqn, as_bytes(payload)); // warm-up: reach the steady owner-buffer size
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish(fqn, as_bytes(payload));
        const auto after  = plexus::testing::alloc_count();
        std::size_t sends = 0;
        for(const auto &ch : channels)
            sends += ch->sends;
        REQUIRE(sends >= static_cast<std::size_t>(K) * subscribers); // every publish fanned to all
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

    constexpr std::uint32_t N = 8;    // ring depth
    constexpr int K           = 1024; // steady-state message count
    const std::string fqn     = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count over a single subscriber, with the topic either
    // latched (depth-N retain in the loop) or not (publish + fan-out only).
    const auto allocs_per_publish = [&](bool latched)
    {
        sink_executor ex;
        sink_channel ch(ex);
        forwarder::peer peer{ch, "node-a"};
        plexus::log::null_logger log_sink;
        forwarder fwd{log_sink};
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

    constexpr int K           = 1024;
    const std::string fqn     = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    sink_channel ch(ex);
    forwarder::peer peer{ch, "node-rx"};

    plexus::log::null_logger log_sink;
    forwarder fwd{log_sink};
    fwd.attach(peer, fqn); // resolves topic_hash -> fqn for the receive tail

    // Build the inner unidirectional payload ONCE (the borrowed receive buffer).
    wire::unidirectional_header uhdr{.source = wire::endpoint_source_type::publisher, .sequence = 1, .topic_hash = wire::fqn_topic_hash(fqn)};
    auto inner = wire::encode_unidirectional(uhdr, as_bytes(payload));

    // The session-assembled metadata half: a stack POD reused every iteration.
    io::message_info info{};
    info.source_timestamp   = 1000;
    info.from_intra_process = false;

    std::size_t seen = 0;
    auto on_message  = [&](std::string_view, std::span<const std::byte>, const io::message_info &) { ++seen; };

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

    REQUIRE(seen == static_cast<std::size_t>(K) + 1); // every deliver reached the callback
    REQUIRE(after - before == 0);                     // zero allocation across the steady-state loop

    // --- source-identity path (gid flag set) ---
    // The inner now carries a varint endpoint counter; deliver decodes it and constructs
    // the publisher_gid IN-PLACE into the stack message_info — the reconstruction must add
    // zero steady-state heap, same as the bytes-only path.
    auto inner_gid       = wire::encode_unidirectional(uhdr, as_bytes(payload), std::uint64_t{0x1234});
    std::size_t gid_seen = 0;
    bool gid_ok          = true;
    auto on_gid          = [&](std::string_view, std::span<const std::byte>, const io::message_info &mi)
    {
        ++gid_seen;
        if(!mi.source_identity || mi.source_identity->endpoint_counter() != 0x1234)
            gid_ok = false;
    };
    fwd.deliver(peer, inner_gid, info, src, /*has_source_identity=*/true, on_gid); // warm-up

    plexus::testing::reset_alloc_count();
    before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.deliver(peer, inner_gid, info, src, /*has_source_identity=*/true, on_gid);
    after = plexus::testing::alloc_count();

    REQUIRE(gid_seen == static_cast<std::size_t>(K) + 1);
    REQUIRE(gid_ok);              // every delivery reconstructed the gid
    REQUIRE(after - before == 0); // gid reconstruction is zero steady-state heap
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
TEST_CASE("steady-state publish through the egress scheduler bands stays frame-once: per-publish "
          "allocation does not scale with the subscriber count",
          "[integration]")
{
    using forwarder = io::message_forwarder<banding_sink_policy>;

    constexpr int K           = 1024;                     // steady-state message count
    const std::size_t warm    = io::detail::k_band_depth; // cycle the in-use band ring once
    const std::string fqn     = "demo._plexus._tcp.local.";
    const std::string payload = "deterministic-steady-state-payload";

    // The per-publish allocation count for a fan-out of N subscribers through the bands.
    const auto allocs_per_publish = [&](int subscribers)
    {
        sink_executor ex;
        std::vector<std::unique_ptr<banding_sink_channel>> channels;
        std::vector<forwarder::peer> peers;
        plexus::log::null_logger log_sink;
        forwarder fwd{log_sink};
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
        const auto after  = plexus::testing::alloc_count();
        std::size_t sends = 0;
        for(const auto &ch : channels)
            sends += ch->sends;
        REQUIRE(sends >= static_cast<std::size_t>(K) * subscribers); // every publish drained to all
        return (after - before) / K;
    };

    // frame-once-fan-to-N through the bands: the same per-publish alloc count whether
    // fanning to 2 or to 8 — the band slots hold the single shared owner by addref, NOT
    // one buffer per destination.
    REQUIRE(allocs_per_publish(2) == allocs_per_publish(8));
}

// The fused peer-liveliness arbiter tick + verdict path is zero-alloc at steady state: once a peer
// has latched alive, a fresh heartbeat stamp followed by evaluate holds the verdict (no transition),
// so settle short-circuits at the latch and never re-enters emit. The default heap-backed storage
// grows only for the first-seen peer (the warm-up); every steady tick reuses the resident node, so
// the per-tick heap delta is zero — the invariant the constrained fixed-capacity path relies on.
TEST_CASE("steady-state fused liveliness tick and verdict path is zero-alloc", "[integration]")
{
    io::liveliness_options opts;
    io::peer_liveliness<> arbiter{opts};

    std::size_t verdicts = 0;
    io::liveliness_verdict latched{io::liveliness_verdict::lost};
    arbiter.add_subscriber();
    arbiter.on_verdict([&](const io::peer_liveliness_event &ev) { latched = ev.verdict; ++verdicts; });

    node_id peer{};
    peer[15] = std::byte{0x5A};

    const std::uint64_t interval_ns = 100'000'000; // the default heartbeat cadence
    std::uint64_t now               = interval_ns;

    // Warm-up: bring the peer up and latch the first (alive) verdict. This is the sole heap growth —
    // the storage inserts the resident node here, outside the measured window.
    arbiter.note_session_up(peer);
    arbiter.note_heartbeat(peer, now);
    arbiter.evaluate(now);
    REQUIRE(verdicts == 1);
    REQUIRE(latched == io::liveliness_verdict::alive);

    constexpr int K = 1024;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
    {
        now += interval_ns;
        arbiter.note_heartbeat(peer, now); // fresh stamp: the peer stays alive
        arbiter.evaluate(now);             // no transition -> settle short-circuits at the latch
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(verdicts == 1);       // the verdict never re-fired: no transition across the loop
    REQUIRE(after - before == 0); // the arbiter tick/verdict path allocates nothing at steady state
}

#ifdef PLEXUS_HAVE_ASIO_MUX
    #include "plexus/io/polymorphic_byte_channel.h"

// The same steady-state no-alloc invariant must hold when the publish loop fans over a
// type-erased polymorphic_byte_channel instead of a concrete channel: the erasure is ONE virtual
// hop per send to the owned concrete channel, minted ONCE at wrap (here at setup), and the adapter
// stores no per-verb callable — so the abstract base adds zero steady-state heap blocks. Measured
// directly over a vector of polymorphic_byte_channels wrapping the inert banding_sink_channel (it
// exposes the backpressured() read the erasure now forwards; no forwarder Policy is needed: the
// gate is the channel layer's per-send behavior).
TEST_CASE("steady-state fan-out over a type-erased polymorphic_byte_channel is zero-alloc", "[integration]")
{
    namespace pio = plexus::io;

    constexpr int N           = 8;
    constexpr int K           = 1024;
    const std::string payload = "deterministic-steady-state-payload";

    sink_executor ex;
    std::vector<std::unique_ptr<pio::polymorphic_byte_channel>> channels;
    std::vector<banding_sink_channel *> sinks; // observers into the owned concrete channels
    channels.reserve(N);
    sinks.reserve(N);
    for(int i = 0; i < N; ++i)
    {
        auto inner = std::make_unique<banding_sink_channel>(ex);
        sinks.push_back(inner.get());
        channels.push_back(std::make_unique<pio::polymorphic_byte_channel>(std::make_unique<pio::channel_adapter<banding_sink_channel>>(std::move(inner))));
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

// The udp best-effort 72 B standalone send is 0-alloc in steady state on the IDLE fast-path:
// when the outbound queue is empty, udp_server::send_standalone_to issues a SYNCHRONOUS
// send_to (the kernel copies the datagram out during the call), so no owned node is
// constructed and no buffer is copied — the caller's scratch is reused immediately. This is
// the path udp_channel::send takes for a whole single datagram (a fragment burst stays on the
// paced queued send_to). Proven over a real bound loopback pair (the sink bound so the sends
// cleanly succeed rather than drawing ICMP noise), warming one send then asserting K idle-path
// sends allocate nothing. K is kept well under the loopback recv-buffer envelope so the path
// never falls back to the async queue; a would_block fallback would show as queued bytes > 0.
TEST_CASE("steady-state udp best-effort 72 B send on the idle fast-path is zero-alloc", "[integration][udp]")
{
    namespace pasio = plexus::asio;

    ::asio::io_context io;
    pasio::udp_server sink{io};
    sink.start(::asio::ip::udp::endpoint{::asio::ip::udp::v4(), 0});

    pasio::udp_server server{io};
    server.start(::asio::ip::udp::endpoint{::asio::ip::udp::v4(), 0});

    const ::asio::ip::udp::endpoint dest{::asio::ip::make_address_v4("127.0.0.1"), sink.port()};
    const std::array<std::byte, 72> payload{};
    const std::span<const std::byte> bytes{payload};

    server.send_standalone_to(bytes, dest);   // warm-up: prime the non-blocking socket
    REQUIRE(server.queued_send_bytes() == 0); // the idle fast-path queued nothing

    constexpr int K = 256;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        server.send_standalone_to(bytes, dest);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(server.queued_send_bytes() == 0); // stayed on the idle fast-path throughout
    REQUIRE(after - before == 0);             // the synchronous send owns no node, copies nothing
}

// The WARM pooled queued path is also 0-alloc in steady state: once a would_block (or any
// non-idle condition) forces the byte-capped send_queue to hold nodes, a burst that re-fills
// the queue reuses the recycled buffer pool (clear()+assign into spilled capacity) instead of
// allocating a fresh vector per datagram. Driven deterministically over the send_queue block
// directly with a sink that DEFERS its completion: a STEADY occupancy is held (the queue never
// drains to empty) while datagrams slide through — one drains (pop + recycle) and one enqueues
// each step, so the freelist hands back a warm buffer every enqueue and the resident depth is
// constant.
//
// The gate is PAYLOAD-SIZE INDEPENDENCE of the steady-state allocation, not delta == 0. The
// buffer recycling is proven by the count being identical for a 72 B and an 8 KiB payload: were
// the node buffers reallocated per datagram, the larger payload's churn would diverge; the
// recycled pool reuses the spilled capacity, so neither size allocates a buffer. The residual,
// payload-independent floor is the per-drain completion callable: send_queue::drive() constructs
// a fresh move_only_function completion every drain turn, and at the project's C++20 floor the
// fallback move_only_function heap-allocates on construction (std::move_only_function with its SBO
// is C++23). That completion allocation is orthogonal to buffer recycling and pre-exists this
// change; a delta == 0 queued path needs the drive-completion made allocation-free (a send-sink
// contract change), which is out of this change's scope. The latency-critical path stays the idle
// fast-path above (genuinely 0-alloc); this queued path is the burst/backpressure regime.
TEST_CASE("steady-state udp send_queue pooled queued path recycles buffers (alloc is payload-size "
          "independent)",
          "[integration][udp]")
{
    using endpoint = ::asio::ip::udp::endpoint;
    using queue    = plexus::datagram::detail::send_queue<endpoint>;
    const endpoint dest{::asio::ip::make_address_v4("127.0.0.1"), 9};

    // The steady-state allocation count of a warm queued path sliding `K` datagrams of the given
    // payload size through a constant-depth backlog (drain one + enqueue one each step).
    const auto steady_allocs = [&](std::span<const std::byte> bytes)
    {
        // The deferred sink: hold the single live completion so the test controls when the front
        // node pops. At most ONE completion is outstanding (the block's serial drive), so a lone
        // member — not a growing container — carries it, keeping the harness itself zero-alloc.
        queue::completion live;
        queue q{[&](std::span<const std::byte>, const endpoint &, queue::completion done) { live = std::move(done); }};

        // Prime a steady backlog: the first enqueue drives the front (stashing the live
        // completion); the rest pile behind it so the deque never reaches the empty state.
        constexpr int depth = 4;
        for(int i = 0; i < depth; ++i)
            REQUIRE(q.enqueue(bytes, dest));
        REQUIRE(q.sending());

        // Warm the freelist + reach the deque's steady node set before measuring.
        constexpr int warm = 16;
        for(int i = 0; i < warm; ++i)
        {
            auto done = std::move(live);
            done(true);
            REQUIRE(q.enqueue(bytes, dest));
        }

        constexpr int K = 1024;
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
        {
            auto done = std::move(live);
            done(true);                      // drain one: pop + recycle into the freelist
            REQUIRE(q.enqueue(bytes, dest)); // enqueue one: pull a warm recycled buffer
        }
        return plexus::testing::alloc_count() - before;
    };

    const std::array<std::byte, 72> small{};
    const std::vector<std::byte> large(8192, std::byte{0x5A});

    // Buffer recycling is proven: the steady-state allocation is IDENTICAL whether the datagram is
    // 72 B or 8 KiB — the node buffer is never reallocated, only its spilled capacity reused.
    REQUIRE(steady_allocs(std::span<const std::byte>{small}) == steady_allocs(std::span<const std::byte>{large}));
}
#endif

#ifdef PLEXUS_HAVE_CRYPTO_DATAGRAM
namespace {

// An inert datagram sink lower channel: it records the sealed ciphertext size without
// copying or allocating, so a datagram_authenticated_channel<sink_lower> send exercises the
// FULL AEAD seal-and-emit path (seq++, nonce, seal into reused scratch, assemble m_send_frame)
// down to the datagram boundary with no transport-side allocation masking the seal's heap.
struct sink_lower
{
    void send(std::span<const std::byte> d)
    {
        last_size = d.size();
        ++sends;
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {"wire", ""};
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
    std::size_t backpressured() const
    {
        return 0;
    }

    std::size_t last_size{0};
    std::size_t sends{0};
};

plexus::crypto::derived_keys datagram_keys()
{
    std::vector<std::byte> psk;
    for(char c : std::string{"a-shared-pre-shared-key-of-decent-length"})
        psk.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>(0x10 + i);
        rs_nonce[i] = static_cast<std::byte>(0xa0 + i);
    }
    for(std::size_t i = 0; i < 32; ++i)
        transcript[i] = static_cast<std::byte>(0x40 + i);
    plexus::crypto::derived_keys k{};
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript, k));
    return k;
}

}

// The AEAD datagram lane inherits the datagram boundary's 0-alloc — but the gate is the PLEXUS
// boundary, NOT OpenSSL's internals. In this tree seal()/open() are OpenSSL EVP, and
// EVP_CIPHER_CTX_new heap-allocates a fresh cipher context on EVERY seal, so a raw delta == 0
// over channel.send is unreachable by contract (the plan excludes crypto-library-internal
// allocations from the gate). The principled gate is therefore RELATIVE: measure the channel's
// per-send allocation against the per-call allocation of a BARE seal() (the OpenSSL cost alone),
// and assert the channel's framing/assembly layer — seq, nonce, the reused m_seal_scratch /
// m_send_frame buffers, the lower send — adds ZERO heap beyond that irreducible OpenSSL floor.
// This isolates the plexus datagram boundary from the AEAD primitive's internals, which is what
// the no-hot-path-alloc invariant governs here.
TEST_CASE("steady-state AEAD datagram send adds zero plexus-side heap beyond the OpenSSL seal floor", "[integration][datagram][dtls]")
{
    namespace pc  = plexus::crypto;
    using channel = pc::datagram_authenticated_channel<sink_lower>;

    const auto keys = datagram_keys();

    // A 72 B application payload behind a plaintext frame header (the AEAD-AAD): the smallest
    // realistic best-effort datagram, matching the UDP fast-path's 72 B gate.
    plexus::wire::frame_header hdr{};
    hdr.type         = plexus::wire::msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = 7;
    hdr.timestamp_ns = 7777;
    const std::array<std::byte, 72> body{};
    hdr.payload_len  = body.size();
    const auto frame = plexus::wire::encode_frame(hdr, body);
    const std::span<const std::byte> frame_view{frame};
    const auto header  = frame_view.first(plexus::wire::header_size);
    const auto payload = frame_view.subspan(plexus::wire::header_size);

    constexpr int K = 1024;

    // Baseline: the per-call OpenSSL allocation floor of a bare seal() into reused scratch
    // (same cipher, key, header-AAD, payload as the channel performs internally). Warm once so
    // the scratch is sized, then measure the steady per-call delta — the irreducible EVP cost.
    const auto seal_allocs = [&]
    {
        std::vector<std::byte> scratch;
        const auto nonce = pc::make_nonce(0, 0);
        REQUIRE(pc::seal(pc::aead_cipher_id::chacha20_poly1305, keys.k_send, nonce, header, payload, scratch));
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
        {
            const auto n = pc::make_nonce(0, static_cast<std::uint64_t>(i));
            REQUIRE(pc::seal(pc::aead_cipher_id::chacha20_poly1305, keys.k_send, n, header, payload, scratch));
        }
        return plexus::testing::alloc_count() - before;
    }();

    // The channel: same seal, plus the plexus framing/assembly boundary on top.
    sink_lower lower;
    channel sealed{lower, pc::aead_cipher_id::chacha20_poly1305, keys};
    sealed.send(frame); // warm-up: size m_seal_scratch + m_send_frame to the steady frame
    REQUIRE(lower.sends == 1);

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        sealed.send(frame);
    const auto channel_allocs = plexus::testing::alloc_count() - before;

    REQUIRE(lower.sends == static_cast<std::size_t>(K) + 1); // every send reached the datagram boundary
    // The plexus datagram boundary adds NOTHING beyond the OpenSSL seal floor: the channel's
    // per-send heap equals a bare seal's, so seq/nonce/frame-assembly/lower-send are all 0-alloc.
    REQUIRE(channel_allocs == seal_allocs);
}
#endif
