// over-limit: one cohesive large-message live-mux round-trip matrix; the per-notifier-variant
// cells share the one cross-process publisher/subscriber mux-acquire harness, and that shared
// fixture preamble alone exceeds the file ceiling, so the cells cannot split across TUs without
// scattering that one harness into over-budget shells.
#include "plexus/shm/futex_notifier_primitive.h"
#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/shm_mux_member.h"
#include "plexus/io/shm/shm_slot_owner.h"
#include "plexus/io/shm/shm_topic_registry.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_template_test_macros.hpp>
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
#include <type_traits>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The large-message live-mux round-trip: a publisher declares a sizing shm_geometry and
// mints the ring through shm_mux_member's ACQUIRE PATH (the live acquire — NOT the
// unit registry), and a >1 MiB then a 16 MiB payload crosses byte-exact to a same-host
// subscriber that attaches the converged ring. Parameterized over the two notifier
// variants (shmn = a real cross-process futex wake; shmr = the reactor-reaped path,
// represented here by the spinning-consumer read the cross-process xproc proofs use —
// the full io_uring reactor wake is covered by the bridge/dual-delivery proofs). Busy-
// poll shm is EXCLUDED. The large cells run >= 2 times in one ctest invocation (the
// no-success-from-a-single-run discipline). The probe-higher cell proves a payload above
// the chosen ceiling FAILS CLOSED under reliable_preserving (no silent best-effort) and
// round-trips under EXPLICIT best_effort_large.

namespace pio = plexus::io::shm;
using plexus::io::endpoint;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

constexpr std::size_t k_mib = 1024 * 1024;

// shmn: a notifier whose signal() issues a real cross-process FUTEX_WAKE on the ring's
// generation word (the futex/parked variant). It binds to the in-region word so the
// wake crosses address spaces.
struct futex_signal_notifier
{
    std::atomic<std::uint32_t> *generation = nullptr;

    explicit futex_signal_notifier(std::atomic<std::uint32_t> *gen = nullptr) noexcept
            : generation(gen)
    {
    }

    void signal() noexcept
    {
        if(generation)
            plexus::shm::notifier_signal(*generation);
    }
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(pio::notifier<futex_signal_notifier>,
              "futex_signal_notifier must satisfy the notifier seam");

// shmr: the reactor-reaped variant. On the publish side it does NOT issue a futex wake
// (the reactor reaps the CQE); the cross-process read here spins on the ring exactly as
// the existing xproc proofs do, so the data path is exercised independent of the wake.
struct spin_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(pio::notifier<spin_notifier>, "spin_notifier must satisfy the notifier seam");

struct coord
{
    std::atomic<std::uint32_t> publisher_ready{0};
    std::atomic<std::uint32_t> subscriber_armed{0};
    std::atomic<std::uint32_t> subscriber_done{0};
    std::atomic<std::uint32_t> bytes_ok{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1,
                     0);
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

// Fill a buffer with a position-dependent byte pattern so a torn or truncated message
// is caught, not just a length mismatch.
std::vector<std::byte> make_payload(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 2654435761u) >> 13);
    return v;
}

// The subscriber process: attach the converged ring (the deterministic name both ends
// derive from the fqn alone), register a cursor at the tail, spin-consume the one
// published message, and compare every byte against the expected pattern.
bool subscribe_and_check(const std::string &fqn, coord *c, const std::vector<std::byte> &expected)
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

    std::uint32_t idx = 0;
    if(ring.register_cursor(idx) != pio::loan_status::ok)
        return false;
    std::uint64_t cursor = ring.tail_position();
    c->subscriber_armed.store(1, std::memory_order_release);

    bool ok = false;
    for(;;)
    {
        pio::broadcast_ring::consume_result out;
        const auto                          st = ring.consume(cursor, out);
        if(st == pio::loan_status::ok)
        {
            ok = out.slab.size() == expected.size() &&
                    std::memcmp(out.slab.data(), expected.data(), expected.size()) == 0;
            break;
        }
        if(st == pio::loan_status::congested)
            ++cursor;
    }
    ring.unregister_cursor(idx);
    return ok;
}

// One large-message round-trip over the LIVE mux path for a notifier variant. The parent
// builds an shm_mux_member, records the sizing geometry for the topic
// (set_topic_geometry), and listen()s — minting + sizing the ring through the member's
// acquire path — then send()s the payload over the handed-up shm_byte_channel. The child
// attaches and verifies byte-exact. Returns whether the bytes crossed intact.
template<typename Notifier>
bool live_mux_roundtrip(const std::string &fqn, std::size_t payload_bytes,
                        pio::ring_geometry_mode mode, std::uint32_t capacity)
{
    const std::vector<std::byte> payload = make_payload(payload_bytes);

    coord *c = map_coord();
    REQUIRE(c != nullptr);

    const pid_t pid = ::fork();
    REQUIRE(pid >= 0);
    if(pid == 0)
    {
        const bool ok = subscribe_and_check(fqn, c, payload);
        c->bytes_ok.store(ok ? 1u : 0u, std::memory_order_release);
        c->subscriber_armed.store(1, std::memory_order_release);
        c->subscriber_done.store(1, std::memory_order_release);
        ::_exit(ok ? 0 : 1);
    }

    using member_t  = pio::shm_mux_member<posix_shm_region_broker, Notifier>;
    using channel_t = typename member_t::channel_type;

    posix_shm_region_broker broker;
    // The binder constructs each ring's notifier over the in-region generation word, so a
    // futex variant's signal() wakes across address spaces (the spin variant ignores it).
    typename member_t::notifier_binder binder = [](std::optional<Notifier>    &slot,
                                                   std::atomic<std::uint32_t> &gen,
                                                   std::atomic<std::uint32_t> &)
    {
        if constexpr(std::is_constructible_v<Notifier, std::atomic<std::uint32_t> *>)
            slot.emplace(&gen);
        else
            slot.emplace();
    };
    member_t member{broker, plexus::io::reliability::reliable, plexus::io::congestion::block,
                    std::move(binder)};

    // The producer-side provisioning: the publisher's effective payload width + mode +
    // capacity reach ring_geometry_for through the live acquire (not the unit registry).
    member.set_topic_geometry(fqn, payload_bytes, pio::shm_geometry{capacity, mode});

    std::unique_ptr<channel_t> channel;
    member.on_accepted([&](std::unique_ptr<channel_t> ch) { channel = std::move(ch); });
    member.listen(endpoint{"shm", fqn});

    if(!channel)
    {
        // The acquire failed closed (the probe-higher reliable leg): unblock the child and
        // report the no-send verdict. The child never sees publisher_ready, so it exits.
        int status = 0;
        c->publisher_ready.store(1, std::memory_order_release); // let the child attach+exit
        while(::waitpid(pid, &status, 0) < 0)
            ;
        ::munmap(c, sizeof(coord));
        return false;
    }

    c->publisher_ready.store(1, std::memory_order_release);
    while(c->subscriber_armed.load(std::memory_order_acquire) == 0)
        ;

    channel->send(std::span<const std::byte>{payload.data(), payload.size()});

    while(c->subscriber_done.load(std::memory_order_acquire) == 0)
        ;
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        ;
    const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
            c->bytes_ok.load(std::memory_order_acquire) == 1u;
    ::munmap(c, sizeof(coord));
    return ok;
}

// Whether the live mux member can mint the ring for (payload, mode, capacity) at all —
// the fail-closed probe. Drives the member's acquire path; no fork. Returns true iff a
// channel was handed up (the ring acquired), and the registry's bound diagnostic.
template<typename Notifier>
struct acquire_probe
{
    bool               channel_minted = false;
    pio::acquire_bound bound          = pio::acquire_bound::none;
};

template<typename Notifier>
acquire_probe<Notifier> probe_acquire(const std::string &fqn, std::size_t payload_bytes,
                                      pio::ring_geometry_mode mode, std::uint32_t capacity)
{
    using member_t  = pio::shm_mux_member<posix_shm_region_broker, Notifier>;
    using channel_t = typename member_t::channel_type;

    posix_shm_region_broker broker;
    member_t member{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    member.set_topic_geometry(fqn, payload_bytes, pio::shm_geometry{capacity, mode});

    std::unique_ptr<channel_t> channel;
    member.on_dialed([&](std::unique_ptr<channel_t> ch, const endpoint &)
                     { channel = std::move(ch); });
    member.dial(endpoint{"shm", fqn});

    acquire_probe<Notifier> r;
    r.channel_minted = static_cast<bool>(channel);
    r.bound          = member.registry().last_acquire_failure().bound;
    return r;
}

}

TEMPLATE_TEST_CASE(
        "shm.large_message a >1 MiB and the 16 MiB payload round-trip over the live mux path",
        "[shm][large_message]", futex_signal_notifier, spin_notifier)
{
    // The large cells run twice in one ctest invocation: the timing/transport
    // reproducibility discipline (no success declared from a single run). A typical
    // (small) fan-out holds depth > capacity under reliable_preserving.
    const std::string base = "topic.large." + std::to_string(::getpid());

    for(int run = 0; run < 2; ++run)
    {
        // > 1 MiB: above the prior ~1 MiB single-message SHM limit.
        REQUIRE(live_mux_roundtrip<TestType>(base + ".over1mib." + std::to_string(run), 2 * k_mib,
                                             pio::ring_geometry_mode::reliable_preserving,
                                             /*capacity=*/2u));

        // The 16 MiB headline at a typical fan-out (capacity 2 -> depth 4 -> 64 MiB,
        // well under the 512 MiB ceiling).
        REQUIRE(live_mux_roundtrip<TestType>(base + ".16mib." + std::to_string(run), 16 * k_mib,
                                             pio::ring_geometry_mode::reliable_preserving,
                                             /*capacity=*/2u));
    }
}

TEMPLATE_TEST_CASE("shm.large_message probe-higher: reliable fails closed above the ceiling, "
                   "explicit best-effort round-trips",
                   "[shm][large_message]", futex_signal_notifier, spin_notifier)
{
    // A payload above the chosen 512 MiB ceiling AT THE FULL FAN-OUT: 17 MiB at capacity
    // 16 is a depth-32 reliable ring (17 MiB * 32 = 544 MiB > 512 MiB ceiling), so
    // reliable_preserving MUST fail closed naming the CEILING bound — never a silent
    // best-effort. The SAME 17 MiB under EXPLICIT best_effort_large is a depth-16 ring
    // (depth == capacity: 17 MiB * 16 = 272 MiB < 512 MiB) and round-trips.
    const std::string base = "topic.probe." + std::to_string(::getpid());

    const acquire_probe<TestType> reliable =
            probe_acquire<TestType>(base + ".reliable", 17 * k_mib,
                                    pio::ring_geometry_mode::reliable_preserving, /*capacity=*/16u);
    REQUIRE_FALSE(reliable.channel_minted);                      // fail-closed, no ring
    REQUIRE(reliable.bound == pio::acquire_bound::slab_ceiling); // the CEILING bound, not OS

    // Explicit best_effort_large at the same payload + capacity round-trips (depth ==
    // capacity keeps the slab under the ceiling). This is the ONLY way best_effort is
    // reached — an explicit mode selection, never a silent fallback from the reliable
    // failure above.
    REQUIRE(live_mux_roundtrip<TestType>(base + ".besteffort", 17 * k_mib,
                                         pio::ring_geometry_mode::best_effort_large,
                                         /*capacity=*/16u));
}
