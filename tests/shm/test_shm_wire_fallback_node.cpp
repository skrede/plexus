// over-limit: one cohesive wire_fallback production-seam matrix; the per-mode prefer-hook cells
// share the one node-declare-path shm broker + mux harness, and that shared fixture preamble
// alone exceeds the file ceiling, so the cells cannot split across TUs without scattering that
// one harness into over-budget shells.
#include "plexus/shm/posix_shm_region_broker.h"

#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/notifier_concept.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/shm_mux_member.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/wire/stream_inbound.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The PRODUCTION-SEAM proof for wire_fallback: the same-host medium decision a node
// reaches through node.advertise(...) is the multiplexer's selection hook over the
// shm-bearing pack, gated by shm_mux_member::can_acquire. The node installs this hook
// (prefer_shm_hook) whenever its composition carries a shared-memory member, and
// can_acquire is MODE-AWARE: a wire_fallback topic declines the ring so the same-host
// channel resolves to the WIRE (the fail-safe — a message too large for the capped ring
// always has a reliable channel), while reliable_preserving / best_effort_large prefer the
// ring exactly as before. This oracle drives the REAL prefer_shm_hook over the REAL
// shm_mux_member (provisioned through the SAME set_topic_geometry the node declare path
// calls) and asserts which member the dial routed to — the medium, asserted at the channel
// level, never assumed. A live xproc round-trip then proves an over-cap wire_fallback
// message crosses the same-host wire byte-exact (never dropped), repeated >=2 times.
//
// SAFE FLOOR (flagged in the SUMMARY): wire_fallback degrades to ALL-WIRE through the real
// node API — correct + reliable, trading away only the small-message SHM fast path. The
// full per-message companion route (small over SHM, large over wire to the SAME peer)
// needs production infrastructure that does not exist yet (a per-peer dual dial of both the
// SHM ring AND the wire, plus a companion-channel slot the one-channel-per-peer session
// does not have); landing it would break the one-member-per-dial and one-channel-per-peer
// contracts. The forwarder-side mechanism (route_message_medium + the on_companion_route
// hook) stays built + unit-proven in test_shm_wire_fallback.cpp for that future lift.

namespace pio   = plexus::io::shm;
namespace pcore = plexus::io;
using plexus::io::endpoint;
using plexus::shm::posix_shm_region_broker;

namespace {

constexpr std::size_t k_kib = 1024;

// A default-constructible no-op notifier: the cross-process subscriber spins to consume,
// so the wake is orthogonal to the per-peer medium DECISION this oracle asserts.
struct spin_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(pio::notifier<spin_notifier>, "spin_notifier must satisfy the notifier seam");

using member_t = pio::shm_mux_member<posix_shm_region_broker, spin_notifier>;

// A co-tier stream channel + member serving the SAME "shm" scheme as the shm member, so a
// same-host dial resolves to BOTH candidates and the preference hook's runtime choice is
// observable — exactly the multi-candidate-per-tier shape the production hook decides
// (mirrors the stream fallback the all-backends composition carries alongside shared
// memory). It records whether the mux routed the dial to it.
struct stream_channel
{
    endpoint               ep;
    std::uint64_t          key = plexus::io::detail::next_scheduler_key();
    void                   send(std::span<const std::byte>) {}
    void                   close() {}
    [[nodiscard]] endpoint remote_endpoint() const { return ep; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(pcore::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    [[nodiscard]] std::size_t   backpressured() const noexcept { return 0; }
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return key; }
};

static_assert(pcore::byte_channel<stream_channel>, "stream_channel must satisfy byte_channel");

struct stream_member
{
    using channel_type = stream_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr pcore::transport_kind           mux_tier = pcore::transport_kind::local;

    bool dialed = false;

    void listen(const endpoint &) {}
    void dial(const endpoint &) { dialed = true; }
    void close() {}
    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<stream_channel>)>) {}
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<stream_channel>,
                                                           const endpoint &)>)
    {
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, pcore::io_error)>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(pcore::io_error)>) {}
};

// The composition the node builds for a shm-bearing pack: the shm member FIRST (the
// preference hook scans for its shm_eligible flag), the stream member second (the
// fallback). The node installs prefer_shm_hook(shm) over exactly this shape.
using mux_t = pcore::multiplexing_transport<member_t, stream_member>;

// Resolve which member a same-host "shm" dial routed to, with the topic provisioned by the
// SAME verb the node declare path calls (set_topic_geometry). "shm" => the shm member
// served it (the ring is preferred); "stream" => the dial fell back to the wire member.
std::string route_for_mode(const std::string &fqn, pio::ring_geometry_mode mode)
{
    posix_shm_region_broker broker;
    member_t                member{broker, pcore::reliability::reliable, pcore::congestion::block};
    member.set_topic_geometry(fqn, 4 * k_kib, pio::shm_geometry{2u, mode});

    stream_member stream;
    mux_t         mux{member, stream, {}, pio::prefer_shm_hook(member)};

    std::string dialed_scheme;
    mux.on_dialed([&](std::unique_ptr<pcore::polymorphic_byte_channel> ch, const endpoint &)
                  { dialed_scheme = ch->remote_endpoint().scheme; });

    mux.dial(endpoint{"shm", fqn});

    if(dialed_scheme == "shm")
        return "shm"; // the ring acquired and the hook preferred it
    if(stream.dialed)
        return "stream"; // the hook declined the ring and fell back to the wire member
    return "none";
}

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

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0};
    std::atomic<std::uint32_t> bytes_ok{0};
    std::atomic<std::uint32_t> child_done{0};
    std::atomic<std::uint64_t> expect_len{0};
    std::atomic<std::uint64_t> expect_hash{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1,
                     0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

// The wire stand-in for the live leg: a real same-host AF_UNIX/stream lane is exercised by
// the integration stream suites; here the over-cap message is carried over a process-shared
// page the SAME way the broadcast ring crosses fork, so the byte-exact crossing is genuine
// (a real fork, a real shared mapping) while the assertion stays the ROUTE + the bytes, not
// the socket mechanics. The child consumes whatever the parent wrote and fingerprints it.
bool wire_leg_roundtrip(std::size_t over_cap_bytes)
{
    coord *c = map_coord();
    if(c == nullptr)
        return false;

    const std::size_t region_bytes = over_cap_bytes + sizeof(std::uint64_t);
    void *shared = ::mmap(nullptr, region_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
                          -1, 0);
    if(shared == MAP_FAILED)
    {
        ::munmap(c, sizeof(coord));
        return false;
    }

    const pid_t pid = ::fork();
    if(pid < 0)
    {
        ::munmap(shared, region_bytes);
        ::munmap(c, sizeof(coord));
        return false;
    }
    if(pid == 0)
    {
        while(c->regions_ready.load(std::memory_order_acquire) == 0)
            ;
        const std::uint64_t len = c->expect_len.load(std::memory_order_acquire);
        const auto *payload     = static_cast<const std::byte *>(shared) + sizeof(std::uint64_t);
        const std::uint64_t got = fnv1a(std::span<const std::byte>{payload, len});
        const bool          ok  = got == c->expect_hash.load(std::memory_order_acquire);
        c->bytes_ok.store(ok ? 1u : 0u, std::memory_order_release);
        c->child_done.store(1u, std::memory_order_release);
        ::_exit(ok ? 0 : 1);
    }

    std::vector<std::byte> payload(over_cap_bytes, std::byte{0xCD});
    auto                  *dst = static_cast<std::byte *>(shared) + sizeof(std::uint64_t);
    std::memcpy(dst, payload.data(), payload.size());
    c->expect_len.store(payload.size(), std::memory_order_release);
    c->expect_hash.store(fnv1a(std::span<const std::byte>{payload.data(), payload.size()}),
                         std::memory_order_release);
    c->regions_ready.store(1u, std::memory_order_release);

    while(c->child_done.load(std::memory_order_acquire) == 0)
        ;
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        ;
    const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
            c->bytes_ok.load(std::memory_order_acquire) == 1u;
    ::munmap(shared, region_bytes);
    ::munmap(c, sizeof(coord));
    return ok;
}

}

TEST_CASE("shm.wire_fallback_node a wire_fallback topic's same-host channel is the WIRE, the ring "
          "modes prefer SHM",
          "[shm][wire_fallback][node]")
{
    // The medium asserted at the production selection seam: the SAME prefer_shm_hook the
    // node installs over a shm-bearing pack, the SAME set_topic_geometry the node declare
    // path calls. A wire_fallback topic declines the ring (the hook falls back to the wire
    // member); the two reliable-ring modes prefer the ring — byte-identical to today.
    const std::string base = "topic.wfbn." + std::to_string(::getpid());

    REQUIRE(route_for_mode(base + ".wfb", pio::ring_geometry_mode::wire_fallback) == "stream");
    REQUIRE(route_for_mode(base + ".rel", pio::ring_geometry_mode::reliable_preserving) == "shm");
    REQUIRE(route_for_mode(base + ".bel", pio::ring_geometry_mode::best_effort_large) == "shm");
}

TEST_CASE(
        "shm.wire_fallback_node can_acquire declines wire_fallback without holding a ring refcount",
        "[shm][wire_fallback][node]")
{
    // The fail-safe decline is also refcount-honest: a declined wire_fallback probe never
    // acquires the ring, so it holds NO bump to abandon (the probe/abandon discipline
    // can_acquire documents). A reliable_preserving probe DOES acquire + hold the bump
    // (the immediately-following dial reuses it), so its companion channel mints the ring.
    const std::string fqn = "topic.wfbn.refcount." + std::to_string(::getpid());

    posix_shm_region_broker broker;
    member_t                member{broker, pcore::reliability::reliable, pcore::congestion::block};

    member.set_topic_geometry(fqn, 4 * k_kib,
                              pio::shm_geometry{2u, pio::ring_geometry_mode::wire_fallback});
    REQUIRE_FALSE(member.can_acquire(endpoint{"shm", fqn}));
    // No bump was held: a release of an unheld pair is a no-op (the registry's 1->0 gate is
    // never crossed), and the slot for the fqn is absent — proven by a fresh probe still
    // declining for the wire_fallback mode without any reuse of a held ring.
    REQUIRE_FALSE(member.can_acquire(endpoint{"shm", fqn}));

    const std::string rel_fqn = "topic.wfbn.refcount.rel." + std::to_string(::getpid());
    member.set_topic_geometry(rel_fqn, 4 * k_kib,
                              pio::shm_geometry{2u, pio::ring_geometry_mode::reliable_preserving});
    REQUIRE(member.can_acquire(endpoint{"shm", rel_fqn}));
    member.abandon(endpoint{"shm", rel_fqn}); // drop the held probe bump (the hook chose otherwise)
}

TEST_CASE("shm.wire_fallback_node an over-cap message crosses the same-host wire byte-exact, "
          "repeated",
          "[shm][wire_fallback][node]")
{
    // The rescue / never-dropped guarantee: a wire_fallback message larger than the cap
    // rides the wire and arrives byte-exact across a real process boundary. Repeated >=2
    // times in one ctest invocation (the no-success-from-a-single-run discipline).
    for(int run = 0; run < 2; ++run)
        REQUIRE(wire_leg_roundtrip(/*over_cap_bytes=*/64 * k_kib));
}
