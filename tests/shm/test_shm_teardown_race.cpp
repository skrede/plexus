#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"
#include "plexus/shm/futex_notifier_primitive.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <utility>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The teardown-race proof (asan-targeted): a PRODUCER process floods the
// in-region notify word with signals at the exact moment the CONSUMER tears the
// channel + bridge down, repeatedly, to drive the wake-vs-teardown race. The
// PASS criterion is asan/ubsan-clean -- no heap-use-after-free on the posted-drain
// path during teardown. The bridge holds the teardown ordering: disarm() cancels
// the asio wait + tears the io_uring (dropping the in-flight futex SQE) + closes
// the eventfd BEFORE the channel (the subscriber the drain touches) is destroyed,
// so a wake racing teardown can never post a drain onto freed state.
// Looped N>=100 over the interleaving; the asan binary is re-run >=3 times.

namespace pio = plexus::io::shm;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

struct test_policy
{
    using executor_type = ::asio::io_context &;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0}; // consumer: regions mapped + armed
    std::atomic<std::uint32_t> tearing_down{0};  // consumer: about to tear down
    std::atomic<std::uint32_t> producer_done{0}; // producer: flood finished
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

constexpr std::uint32_t k_payload = 0xFEEDFACEu;

// The producer child: attach, publish ONE value, then FLOOD the notify word with
// signals across the consumer's teardown window so a wake is maximally likely to
// be in flight as the consumer disarms.
bool flood(const std::string &fqn, coord *c)
{
    posix_shm_region_broker broker;
    region_handle           ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok ||
       broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;

    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::best_effort,
                              plexus::io::congestion::drop_newest, claim) == pio::loan_status::ok)
    {
        std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
        ring.commit(claim.position, sizeof(k_payload));
    }

    // Signal continuously through (and past) the consumer's teardown window: a wake
    // landing exactly as disarm() runs is the race the asan proof must clear.
    for(int i = 0; i < 2000; ++i)
    {
        plexus::shm::notifier_signal(ring.notify_generation());
        if(c->tearing_down.load(std::memory_order_acquire) == 1 && i > 50)
            break; // a few more after the consumer began tearing down, then stop
    }
    // A final burst strictly during/after the teardown signal.
    for(int i = 0; i < 64; ++i)
        plexus::shm::notifier_signal(ring.notify_generation());
    c->producer_done.store(1, std::memory_order_release);
    return true;
}

}

TEST_CASE("shm.teardown_race a wake racing teardown never touches freed state",
          "[shm][teardown_race]")
{
    const std::string        fqn  = "topic.teardown." + std::to_string(::getpid());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 100; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);

        if(pid == 0)
        {
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ;
            const bool ok = flood(fqn, c);
            ::_exit(ok ? 0 : 1);
        }

        posix_shm_region_broker broker;
        region_handle           ctrl, slab;
        REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count),
                              pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn),
                              pio::slab_region_bytes(geom.cell_count, geom.slot_capacity),
                              pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count,
                                            geom.slot_capacity, ring) == pio::loan_status::ok);

        ::asio::io_context io;
        // The channel + bridge live in a nested scope so their destruction order
        // (channel destructs while the bridge is still alive but DISARMED) is the
        // teardown the proof exercises; the explicit disarm() before scope exit is
        // the stop-first edge.
        {
            plexus::asio::shm::ring_notifier<test_policy> bridge(io, ring.notify_generation());
            pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>> channel(
                    ring, bridge, plexus::io::reliability::best_effort,
                    plexus::io::congestion::drop_newest);

            bridge.arm(
                    [&]
                    {
                        pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>>::deliver_fn
                                deliver = [](plexus::wire_bytes<pio::shm_slot_owner>) {};
                        channel.drain(deliver); // touches the subscriber -- must be alive here
                    });

            c->regions_ready.store(1, std::memory_order_release);

            // Pump the reactor briefly so wakes are actively being posted/drained,
            // THEN tear down mid-flood: disarm (stop the watch) while the producer is
            // still signaling, then let the channel destruct. A drain posted but not
            // yet run, or a CQE in flight, must not touch the destroyed subscriber.
            io.run_for(std::chrono::milliseconds(2));
            c->tearing_down.store(1, std::memory_order_release);
            bridge.disarm(); // stop -> (io_uring exit drops the SQE) -> close fd
            io.poll();       // run any drain already posted BEFORE the channel dies
        } // channel + bridge destruct here (bridge dtor disarm is a no-op)

        // Drain any residual posted handlers on a now-disarmed reactor (none should
        // touch freed state; this flushes the queue so asan sees a clean teardown).
        io.poll();

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);

        ::munmap(c, sizeof(coord));
    }
}
