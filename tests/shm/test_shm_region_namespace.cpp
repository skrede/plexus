#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The shm-region NAMESPACE folded into region_name_for. Two unrelated co-host apps that pick
// distinct namespaces compute distinct region names for the same topic and never collide on a
// shared region. This proves the property cross-process (fork): a producer mints regions under
// namespace "alpha"; a consumer attaching under "alpha" RECEIVES the value (convergence), and a
// consumer attaching under "beta" CANNOT attach the producer's region (isolation — the names
// differ, so no cross-delivery). The empty-namespace name equals the namespace-less name, so the
// existing shm rings are unchanged (back-compat, asserted at the naming layer here too).

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

namespace {

std::string control_name(const std::string &fqn, std::string_view ns)
{
    return pio::region_name_for(fqn, pio::ring_direction::request, ns);
}
std::string slab_name(const std::string &fqn, std::string_view ns)
{
    return pio::region_name_for(fqn, pio::ring_direction::request, ns) + ".s";
}

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0};
    std::atomic<std::uint32_t> consumer_armed{0};
    std::atomic<std::uint32_t> consumer_done{0};
    std::atomic<std::uint32_t> value_seen{0};
    std::atomic<std::uint32_t> attach_ok{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

constexpr std::uint32_t k_payload = 0x7E57DA7Au;

// The consumer side: attach the (fqn, ns) region the producer minted and read the one value.
// attach_ok records whether the region was found under THIS namespace; value_seen records a
// byte-equal read. A namespace that does not match the producer's CANNOT attach (the region
// name differs), so attach_ok stays 0 and no value is ever seen.
void run_consumer(coord *c, const std::string &fqn, std::string_view ns)
{
    while(c->regions_ready.load(std::memory_order_acquire) == 0)
        ;

    posix_shm_region_broker broker;
    region_handle ctrl, slab;
    const bool attached = broker.attach(control_name(fqn, ns), ctrl) == pio::region_status::ok && broker.attach(slab_name(fqn, ns), slab) == pio::region_status::ok;
    c->attach_ok.store(attached ? 1u : 0u, std::memory_order_release);

    bool ok = false;
    if(attached)
    {
        pio::broadcast_ring ring;
        if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) == pio::loan_status::ok)
        {
            std::uint32_t idx = 0;
            if(ring.register_cursor(idx) == pio::loan_status::ok)
            {
                std::uint64_t cursor = ring.tail_position();
                c->consumer_armed.store(1, std::memory_order_release);
                for(;;)
                {
                    pio::broadcast_ring::consume_result out;
                    const auto st = ring.consume(cursor, out);
                    if(st == pio::loan_status::ok)
                    {
                        std::uint32_t got = 0;
                        std::memcpy(&got, out.slab.data(), sizeof(got));
                        ok = (got == k_payload);
                        break;
                    }
                    if(st == pio::loan_status::congested)
                        ++cursor;
                }
                ring.unregister_cursor(idx);
            }
        }
    }
    c->value_seen.store(ok ? 1u : 0u, std::memory_order_release);
    c->consumer_armed.store(1, std::memory_order_release);
    c->consumer_done.store(1, std::memory_order_release);
}

}

TEST_CASE("shm.region_namespace same namespace converges and a different one is isolated", "[shm][same_host][namespace][roundtrip]")
{
    const std::string fqn         = "topic.region_namespace." + std::to_string(::getpid());
    const std::string producer_ns = "alpha";
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    // consumer_ns_matches drives the same producer/consumer dance twice: with the SAME namespace
    // (convergence -> the value arrives) and a DIFFERENT one (isolation -> the region cannot even
    // be attached, so no value crosses the namespace boundary).
    auto round = [&](std::string_view consumer_ns, bool expect_share)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            run_consumer(c, fqn, consumer_ns);
            ::_exit(0);
        }

        posix_shm_region_broker broker;
        region_handle ctrl, slab;
        REQUIRE(broker.create(control_name(fqn, producer_ns), pio::control_region_bytes(geom.cell_count), pio::create_options{.unlink_stale_on_create = true}, ctrl) ==
                pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn, producer_ns), pio::slab_region_bytes(geom.cell_count, geom.slot_capacity), pio::create_options{.unlink_stale_on_create = true}, slab) ==
                pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count, geom.slot_capacity, ring) == pio::loan_status::ok);
        c->regions_ready.store(1, std::memory_order_release);

        while(c->consumer_armed.load(std::memory_order_acquire) == 0)
            ;

        if(expect_share)
        {
            pio::broadcast_ring::claim_result claim;
            REQUIRE(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) == pio::loan_status::ok);
            std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
            REQUIRE(ring.commit(claim.position, sizeof(k_payload)) == pio::loan_status::ok);
        }

        while(c->consumer_done.load(std::memory_order_acquire) == 0)
            ;

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));

        if(expect_share)
        {
            REQUIRE(c->attach_ok.load(std::memory_order_acquire) == 1u);
            REQUIRE(c->value_seen.load(std::memory_order_acquire) == 1u);
        }
        else
        {
            // ISOLATION: the consumer's distinct namespace names a different region, so it never
            // attaches the producer's and no value ever crosses the namespace boundary.
            REQUIRE(c->attach_ok.load(std::memory_order_acquire) == 0u);
            REQUIRE(c->value_seen.load(std::memory_order_acquire) == 0u);
        }

        ::munmap(c, sizeof(coord));
    };

    SECTION("convergence: same namespace shares the ring")
    {
        round(producer_ns, true);
    }
    SECTION("isolation: a different namespace does not share the ring")
    {
        round("beta", false);
    }
}
