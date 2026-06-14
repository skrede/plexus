#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/notifier_concept.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/shm_mux_member.h"
#include "plexus/io/shm/same_host.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"
#include "plexus/topic_qos.h"
#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The wire_fallback per-message routing proof, driven through BOTH the live
// shm_mux_member acquire path (the capped ring is minted + sized by the member) AND
// the message_forwarder publish fan-out (the per-message size route). A topic in
// wire_fallback mode keeps a capped SHM ring alongside a same-host wire channel; the
// forwarder routes a message that fits the cap over the ring (zero-copy) and a message
// larger than the cap over the wire. The carrying medium is ASSERTED per message
// (a per-channel tap records which medium took it), not assumed: the small message
// rides the ring (verified byte-exact cross-process), the large message rides the wire.
// The over-cap (wire-routed) cell repeats in one ctest invocation (the
// no-success-from-a-single-run discipline). The rescue cell proves a wire_fallback
// subscriber with NO companion ring still receives every message over the wire — the
// ring is the small-message fast path, never a correctness dependency.

namespace pio = plexus::io::shm;
using plexus::io::endpoint;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

constexpr std::size_t k_kib = 1024;

// A default-constructible no-op notifier: the cross-process subscriber spins to consume
// (as every existing xproc proof does), so the wake is orthogonal to the per-message
// ROUTE this test asserts. The real cross-process futex wake is covered by the
// bridge/dual-delivery proofs.
struct spin_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(pio::notifier<spin_notifier>, "spin_notifier must satisfy the notifier seam");

using member_t    = pio::shm_mux_member<posix_shm_region_broker, spin_notifier>;
using shm_channel = typename member_t::channel_type;

// The carrying medium a published message took, recorded by the tap so the test can
// PROVE (not assume) which lane a given message rode.
enum class medium
{
    wire,
    shm,
};

// A 64-bit FNV-1a over a byte span — a cheap content fingerprint the child reports back
// so the parent proves the SHM ring delivered the EXACT framed bytes the forwarder sent,
// without re-deriving the frame layout in the child.
std::uint64_t fnv1a(std::span<const std::byte> bytes) noexcept
{
    std::uint64_t h = 1469598103934665603ull;
    for(std::byte b : bytes)
    {
        h ^= static_cast<std::uint8_t>(b);
        h *= 1099511628211ull;
    }
    return h;
}

// A delivery tap satisfying byte_channel. In the WIRE role it only records that the
// message took the wire (no real transport — the wire lane's own delivery is covered by
// the stream-transport suites; here the assertion is the ROUTE). In the SHM-companion
// role it wraps the live shm_byte_channel the mux member minted and forwards send()
// straight into the ring (the real cross-process SHM path) while recording the framed
// bytes for the byte-exact fingerprint. It deliberately exposes NO backpressured(), so
// the forwarder's egress short-circuits to a direct synchronous send (the tap fires
// inline, deterministically). plexus::detail::move_only_function is fully qualified
// because io::detail shadows plexus::detail inside namespace plexus::io.
class tap_channel
{
public:
    tap_channel(medium kind, std::string scheme, shm_channel *ring = nullptr)
        : m_kind(kind), m_scheme(std::move(scheme)), m_ring(ring)
    {
    }

    void send(std::span<const std::byte> data)
    {
        m_carried.push_back(m_kind);
        m_last_len  = data.size();
        m_last_hash = fnv1a(data);
        if(m_kind == medium::shm && m_ring != nullptr)
            m_ring->send(data);
    }

    void close() {}
    [[nodiscard]] endpoint remote_endpoint() const { return endpoint{m_scheme, "peer"}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    [[nodiscard]] std::size_t count(medium m) const
    {
        std::size_t n = 0;
        for(medium c : m_carried)
            if(c == m)
                ++n;
        return n;
    }
    [[nodiscard]] std::size_t total() const { return m_carried.size(); }
    [[nodiscard]] std::size_t last_len() const { return m_last_len; }
    [[nodiscard]] std::uint64_t last_hash() const { return m_last_hash; }

    // Forget pre-publish control traffic (the subscribe_response the attach emits over
    // the wire channel) so the carried tally counts only data publishes.
    void reset() { m_carried.clear(); }

private:
    medium                m_kind;
    std::string           m_scheme;
    shm_channel          *m_ring;
    std::vector<medium>   m_carried;
    std::size_t           m_last_len  = 0;
    std::uint64_t         m_last_hash = 0;
};

static_assert(plexus::io::byte_channel<tap_channel>, "tap_channel must satisfy byte_channel");

// A test Policy binding the tap as the byte_channel, reusing the deterministic inproc
// executor/timer substrate the forwarder needs for its egress scheduler.
struct tap_policy
{
    using executor_type     = plexus::inproc::inproc_executor<> &;
    using byte_channel_type = tap_channel;
    using timer_type        = plexus::inproc::inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<tap_policy>, "tap_policy must satisfy Policy");

using forwarder = plexus::io::message_forwarder<tap_policy>;

struct coord
{
    std::atomic<std::uint32_t> publisher_ready{0};
    std::atomic<std::uint32_t> subscriber_armed{0};
    std::atomic<std::uint32_t> subscriber_done{0};
    std::atomic<std::uint64_t> expect_len{0};
    std::atomic<std::uint64_t> expect_hash{0};
    std::atomic<std::uint32_t> bytes_ok{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}
std::string slab_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request) + ".s";
}

// The SHM subscriber process: attach the converged ring (the deterministic name both
// ends derive from the fqn alone), register a cursor at the tail, spin-consume the one
// small message the forwarder routed over the ring, and fingerprint it against the
// framed bytes the parent recorded — proving byte-exact cross-process SHM delivery of
// the actual forwarder frame.
bool subscribe_and_check(const std::string &fqn, coord *c)
{
    while(c->publisher_ready.load(std::memory_order_acquire) == 0)
        ;

    posix_shm_region_broker broker;
    region_handle           ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok ||
       broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;
    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    const std::uint64_t want_len  = c->expect_len.load(std::memory_order_acquire);
    const std::uint64_t want_hash = c->expect_hash.load(std::memory_order_acquire);

    std::uint64_t cursor = 0;
    bool          ok     = false;
    for(;;)
    {
        pio::broadcast_ring::consume_result out;
        const auto st = ring.consume(cursor, out);
        if(st == pio::loan_status::ok)
        {
            ok = out.slab.size() == want_len && fnv1a(out.slab) == want_hash;
            break;
        }
        if(st == pio::loan_status::congested)
            ++cursor;
    }
    return ok;
}

// Drive one wire_fallback round-trip through the LIVE mux acquire + the forwarder
// publish route. The parent mints the capped ring through shm_mux_member::listen, wires
// the forwarder's companion-route hook to route_message_medium against the member's
// resolved_geometry_for(fqn), attaches a wire subscriber, then publishes a small (<=
// cap) and a large (> cap) message. A forked subscriber verifies the small message
// arrived byte-exact over the SHM ring. Returns the two wire taps so the caller asserts
// the per-message medium.
struct route_result
{
    std::size_t shm_carried   = 0;   // messages the SHM-companion tap took
    std::size_t wire_carried  = 0;   // messages the wire tap took
    bool        shm_byte_exact = false;
    bool        subscriber_exited_clean = false;
};

route_result drive_round_trip(const std::string &fqn, std::size_t declared_payload,
                              std::uint32_t capacity, std::size_t small_bytes,
                              std::size_t large_bytes)
{
    std::vector<std::byte> small(small_bytes, std::byte{0xAB});
    std::vector<std::byte> large(large_bytes, std::byte{0xCD});

    coord *c = map_coord();
    REQUIRE(c != nullptr);

    const pid_t pid = ::fork();
    REQUIRE(pid >= 0);
    if(pid == 0)
    {
        const bool ok = subscribe_and_check(fqn, c);
        c->bytes_ok.store(ok ? 1u : 0u, std::memory_order_release);
        c->subscriber_done.store(1, std::memory_order_release);
        ::_exit(ok ? 0 : 1);
    }

    posix_shm_region_broker broker;
    member_t member{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    member.set_topic_geometry(fqn, declared_payload, pio::shm_geometry{capacity,
                                                                       pio::ring_geometry_mode::wire_fallback});

    std::unique_ptr<shm_channel> companion_ring;
    member.on_accepted([&](std::unique_ptr<shm_channel> ch) { companion_ring = std::move(ch); });
    member.listen(endpoint{"shm", fqn});
    REQUIRE(companion_ring);

    // The two delivery lanes for this same-host wire_fallback subscriber: the wire tap
    // is the recorded sub.channel (the fail-safe default), the shm tap wraps the live
    // companion ring. The forwarder routes per message between them.
    tap_channel wire_tap{medium::wire, "unix"};
    tap_channel shm_tap{medium::shm, "unix", companion_ring.get()};

    forwarder fwd;
    fwd.declare(fqn, plexus::topic_qos{.reliability = plexus::io::reliability::reliable});

    // The companion-route hook: consult route_message_medium against the member's
    // resolved {mode, cap}. shm -> the SHM-companion tap (it forwards into the ring),
    // stream -> nullptr (the forwarder keeps the message on the wire sub.channel). This
    // is the recommended reversible default from the plan: the wire stays the recorded
    // channel, the ring is an additive consult.
    fwd.on_companion_route(
        [&](std::string_view, std::string_view route_fqn, std::size_t bytes) -> tap_channel * {
            const auto g = member.resolved_geometry_for(std::string{route_fqn});
            return pio::route_message_medium(g.mode, bytes, g.slot_capacity) ==
                           pio::same_host_medium::shm
                       ? &shm_tap
                       : nullptr;
        });

    forwarder::peer p{wire_tap, "peer-node"};
    fwd.attach_for_fanout(p, fqn);
    wire_tap.reset();   // drop the attach's subscribe_response; count only data publishes

    // Publish the SMALL message first: it fits the cap, so the route picks the ring; the
    // tap records the framed bytes it forwarded so the child can fingerprint them.
    fwd.publish(fqn, std::span<const std::byte>{small.data(), small.size()});
    c->expect_len.store(shm_tap.last_len(), std::memory_order_release);
    c->expect_hash.store(shm_tap.last_hash(), std::memory_order_release);
    c->publisher_ready.store(1, std::memory_order_release);

    // Publish the LARGE message: it exceeds the cap, so the route keeps it on the wire.
    fwd.publish(fqn, std::span<const std::byte>{large.data(), large.size()});

    while(c->subscriber_done.load(std::memory_order_acquire) == 0)
        ;
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        ;

    route_result r;
    r.shm_carried             = shm_tap.count(medium::shm);
    r.wire_carried            = wire_tap.count(medium::wire);
    r.shm_byte_exact          = c->bytes_ok.load(std::memory_order_acquire) == 1u;
    r.subscriber_exited_clean = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    ::munmap(c, sizeof(coord));
    return r;
}

}

TEST_CASE("shm.wire_fallback small rides the SHM ring, large rides the wire — medium asserted",
          "[shm][wire_fallback]")
{
    // A capped ring at a small declared payload: ring_geometry_for keeps the small-tier
    // slot stride (4096), so the cap is 4096. A 1 KiB message fits the cap (SHM); a 64
    // KiB message exceeds it (wire). The over-cap (wire-routed) cell repeats twice in one
    // ctest invocation — the no-success-from-a-single-run discipline.
    const std::string base = "topic.wfb." + std::to_string(::getpid());

    for(int run = 0; run < 2; ++run)
    {
        const route_result r = drive_round_trip(base + ".run." + std::to_string(run),
                                                /*declared_payload=*/2 * k_kib, /*capacity=*/2u,
                                                /*small=*/1 * k_kib, /*large=*/64 * k_kib);

        // The small message rode the SHM ring (zero-copy fast path), byte-exact across
        // the process boundary; the large message rode the wire (rerouted, not dropped).
        REQUIRE(r.shm_carried == 1);
        REQUIRE(r.wire_carried == 1);
        REQUIRE(r.shm_byte_exact);
        REQUIRE(r.subscriber_exited_clean);
    }
}

TEST_CASE("shm.wire_fallback a ring-less subscriber receives BOTH messages over the wire (rescue)",
          "[shm][wire_fallback]")
{
    // The dual-delivery rescue: a wire_fallback subscriber whose companion ring is absent
    // (the hook never returns one) still receives EVERY message over its recorded wire
    // channel — the ring is the small-message fast path, never a correctness dependency.
    // No SHM region is minted here; the route helper still runs (against the resolved
    // geometry) but the hook withholds the companion, so both messages take the wire.
    const std::string fqn = "topic.wfb.rescue." + std::to_string(::getpid());

    std::vector<std::byte> small(1 * k_kib, std::byte{0xAB});
    std::vector<std::byte> large(64 * k_kib, std::byte{0xCD});

    tap_channel wire_tap{medium::wire, "unix"};

    forwarder fwd;
    fwd.declare(fqn, plexus::topic_qos{.reliability = plexus::io::reliability::reliable});
    // The companion is ABSENT: the hook always declines (nullptr), so the forwarder keeps
    // every message on the recorded wire sub.channel — never a dropped large message.
    fwd.on_companion_route(
        [](std::string_view, std::string_view, std::size_t) -> tap_channel * { return nullptr; });

    forwarder::peer p{wire_tap, "peer-node"};
    fwd.attach_for_fanout(p, fqn);
    wire_tap.reset();   // drop the attach's subscribe_response; count only data publishes

    fwd.publish(fqn, std::span<const std::byte>{small.data(), small.size()});
    fwd.publish(fqn, std::span<const std::byte>{large.data(), large.size()});

    REQUIRE(wire_tap.count(medium::wire) == 2);
    REQUIRE(wire_tap.count(medium::shm) == 0);
}
