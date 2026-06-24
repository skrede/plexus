#include "plexus/asio/same_host_transports.h"

#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/region_naming.h"

#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The portable same-host composition: the consumer writes the SAME code on every platform
// (no #if), and on Linux it resolves to the shm + AF_UNIX + TCP fast path. Two things are
// proven here. FIRST, the portable type stands up a live node (constructs the shm-bearing set
// behind the header, mints a node, and brings up a real same-host listener). SECOND, the
// broker-sharing model the portable type depends on holds end to end: two INDEPENDENT
// posix_shm_region_brokers — one per process, the exact per-instance broker the portable type
// owns — converge on the SAME deterministically-named region with no exchange and a value
// round-trips byte-equal cross-address-space over the MAP_SHARED ring. The portable type owns
// one such broker per instance, so two peers share an shm ring by topic name.

namespace pasio = plexus::asio;
namespace pio   = plexus::io::shm;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0};
    std::atomic<std::uint32_t> consumer_armed{0};
    std::atomic<std::uint32_t> consumer_done{0};
    std::atomic<std::uint32_t> value_seen{0};
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

constexpr std::uint32_t k_payload = 0x5A4E0FEEu;

}

TEST_CASE("shm.same_host_transports the portable composition stands up a live node",
          "[shm][mux][node][same_host_transports]")
{
    ::asio::io_context                  io;
    plexus::discovery::static_discovery disc{{}};

    pasio::same_host_transports ts{io};

    plexus::node_id id{};
    id[0]     = std::byte{0x2A};
    auto node = ts.make_node(disc, id, plexus::node_options{});

    const std::string sock = "/tmp/plexus-same-host-" + std::to_string(::getpid()) + ".sock";
    node.listen({"unix", sock});
    io.poll();

    SUCCEED("same_host_transports minted a live node and stood up a same-host listener");
}

TEST_CASE("shm.same_host_transports two independent brokers share an shm ring by name",
          "[shm][same_host_transports][roundtrip]")
{
    const std::string        fqn  = "topic.same_host_transports." + std::to_string(::getpid());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 100; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);

        if(pid == 0)
        {
            bool ok = false;
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ;

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
                        std::uint64_t cursor = ring.tail_position();
                        c->consumer_armed.store(1, std::memory_order_release);

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
                                ++cursor;
                        }
                        ring.unregister_cursor(idx);
                    }
                    else
                        c->consumer_armed.store(1, std::memory_order_release);
                }
            }
            c->value_seen.store(ok ? 1u : 0u, std::memory_order_release);
            c->consumer_armed.store(1, std::memory_order_release);
            c->consumer_done.store(1, std::memory_order_release);
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
        c->regions_ready.store(1, std::memory_order_release);

        while(c->consumer_armed.load(std::memory_order_acquire) == 0)
            ;

        pio::broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable,
                                       plexus::io::congestion::block,
                                       claim) == pio::loan_status::ok);
        std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
        REQUIRE(ring.commit(claim.position, sizeof(k_payload)) == pio::loan_status::ok);

        while(c->consumer_done.load(std::memory_order_acquire) == 0)
            ;

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
        REQUIRE(c->value_seen.load(std::memory_order_acquire) == 1u);

        ::munmap(c, sizeof(coord));
    }
}
