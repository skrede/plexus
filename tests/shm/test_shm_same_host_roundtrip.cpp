#include "plexus/native/machine_fingerprint.h"
#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/region_naming.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The first whole-stack cross-process proof: TWO processes converge on the SAME
// deterministically-named regions with NO name exchange (the demand-driven
// convergence), one CREATES the broker regions + drives the broadcast_ring as
// producer, the other ATTACHES the same names + drives the ring as consumer, and a
// real value round-trips byte-equal cross-address-space over the MAP_SHARED ring.
// The machine fingerprint, the broker, and the ring are all proven together.
// Looped N>=100; the ctest binary is re-run >=3 times for reproducibility.

namespace pio = plexus::io::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

namespace {

// A tiny shared coordination block (anonymous MAP_SHARED, inherited across fork):
// the creator announces the regions are live; the consumer announces it has
// finished so the creator can tear down without racing the attacher's munmap.
struct coord
{
    std::atomic<std::uint32_t> regions_ready{0};  // creator: regions mapped + ring stamped
    std::atomic<std::uint32_t> consumer_armed{0}; // consumer: cursor registered at the tail
    std::atomic<std::uint32_t> consumer_done{0};  // consumer: finished reading
    std::atomic<std::uint32_t> value_seen{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1,
                     0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

// The two region names a (fqn) ring maps over: the control+cells region and the
// payload slab. BOTH ends compute these identically from the fqn alone (the
// control name is the bare-hex region name; the slab appends a ".s" discriminator
// so the two regions never collide).
std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}
std::string slab_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request) + ".s";
}

constexpr std::uint32_t k_payload = 0xC0FFEEu;

}

TEST_CASE("shm.same_host_roundtrip two processes round-trip a value over a named ring",
          "[shm][same_host_roundtrip]")
{
    // The fqn unique to this process so concurrent ctest shards never collide.
    const std::string        fqn  = "topic.roundtrip." + std::to_string(::getpid());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 100; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);

        if(pid == 0)
        {
            // CONSUMER child: independently compute the SAME names, attach, register
            // a cursor at the producer's tail, and consume the published value.
            bool ok = false;
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ; // wait for the creator to stamp the regions

            posix_shm_region_broker broker;
            region_handle           ctrl, slab;
            if(broker.attach(control_name(fqn), ctrl) == pio::region_status::ok &&
               broker.attach(slab_name(fqn), slab) == pio::region_status::ok)
            {
                pio::broadcast_ring ring;
                if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) ==
                   pio::loan_status::ok)
                {
                    std::uint32_t idx = 0;
                    if(ring.register_cursor(idx) == pio::loan_status::ok)
                    {
                        // Snapshot the cursor at the tail BEFORE announcing armed, so
                        // the producer publishes only after this start point is fixed:
                        // the message lands at exactly this position and is never
                        // missed (no publish-before-arm race).
                        std::uint64_t cursor = ring.tail_position();
                        c->consumer_armed.store(1, std::memory_order_release);

                        // Spin-consume until the one published message arrives.
                        for(;;)
                        {
                            pio::broadcast_ring::consume_result out;
                            const auto                          st = ring.consume(cursor, out);
                            if(st == pio::loan_status::ok)
                            {
                                std::uint32_t got = 0;
                                std::memcpy(&got, out.slab.data(), sizeof(got));
                                ok = (got == k_payload);
                                break;
                            }
                            if(st == pio::loan_status::congested)
                                ++cursor; // a tombstone: step forward
                        }
                        ring.unregister_cursor(idx);
                    }
                    else
                        c->consumer_armed.store(1, std::memory_order_release);
                }
            }
            c->value_seen.store(ok ? 1u : 0u, std::memory_order_release);
            // Unblock the producer on every exit path (incl. an attach failure that
            // never reached the arm point) so the parent never hangs.
            c->consumer_armed.store(1, std::memory_order_release);
            c->consumer_done.store(1, std::memory_order_release);
            ::_exit(ok ? 0 : 1);
        }

        // CREATOR parent: create the two regions, build the ring, publish ONE value,
        // wait for the consumer to read it, then tear down.
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
        c->regions_ready.store(1, std::memory_order_release);

        // Wait for the consumer to register its cursor at the tail BEFORE publishing,
        // so the message lands at a position the consumer is already watching (the
        // reclamation gate also sees the registered cursor). This closes the
        // publish-before-arm race that would otherwise leave the value unread.
        while(c->consumer_armed.load(std::memory_order_acquire) == 0)
            ;

        // Publish the value (reliable claim -> fill -> commit).
        pio::broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                       plexus::io::congestion::block,
                                       claim) == pio::loan_status::ok);
        std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
        REQUIRE(ring.commit(claim.position, sizeof(k_payload)) == pio::loan_status::ok);

        while(c->consumer_done.load(std::memory_order_acquire) == 0)
            ; // hold the regions live until the consumer has read

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
        REQUIRE(c->value_seen.load(std::memory_order_acquire) == 1u);

        ::munmap(c, sizeof(coord));
    }
}
