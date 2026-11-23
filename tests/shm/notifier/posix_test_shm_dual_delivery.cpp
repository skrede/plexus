#include "region_names.h"
#include "posix_test_shm_dual_delivery_common.h"

#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/native/region_handle.h"
#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/wire_bytes.h"

#include "plexus/asio/io_context_verbs.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <sys/mman.h>
#include <sys/wait.h>

#include <span>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <cstdint>
#include <cstring>

// Dual-delivery (xproc): the same-host shared-memory upgrade ADDS a fast path; it
// does NOT REPLACE the wire path. For ONE topic, this oracle proves both flows coexist:
//   * the SHM leg — a forked producer publishes + wakes the consumer's parked slot; the
//     parent's io_uring-futex bridge reaps the cross-process wake on its OWN asio reactor
//     and the drain delivers the value over the shm_channel.
//   * the WIRE leg — for the SAME topic, a loopback TCP peer delivers the SAME bytes over
//     a real socket; the SHM upgrade never suppressed it.
// Both are keyed to one topic: the publisher's ring name and the loopback peer carry the same fqn. Looped N>=3 in-body.

using namespace dual_delivery_fixture;

namespace {

struct test_policy : plexus::asio::io_context_verbs
{
};

}

TEST_CASE("shm.dual_delivery a same-host SHM topic and a cross-host wire topic coexist on one topic", "[shm][dual_delivery]")
{
    const std::string fqn         = "topic.dual." + std::to_string(plexus::testing::process_id());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 3; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);

        if(pid == 0)
        {
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ;
            const bool ok = produce_shm(fqn);
            ::_exit(ok ? 0 : 1);
        }

        posix_shm_region_broker broker;
        region_handle ctrl, slab;
        const plexus::testing::unlink_on_exit names{ctrl, slab};
        REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count), pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn), pio::slab_region_bytes(geom.cell_count, geom.slot_capacity), pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count, geom.slot_capacity, plexus::testing::test_publisher(), ring) == pio::loan_status::ok);

        ::asio::io_context io;
        plexus::asio::shm::ring_notifier<test_policy> bridge(io, ring.header(), "");
        pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>> channel(ring, bridge, plexus::io::reliability::reliable, plexus::io::congestion::block);

        std::uint32_t shm_got = 0;
        bridge.arm(*channel.engage_subscriber(),
                   [&]
                   {
                       pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>>::deliver_fn deliver = [&](plexus::wire_bytes<pio::shm_slot_owner> wb)
                       {
                           std::memcpy(&shm_got, wb.data(), sizeof(shm_got));
                           c->shm_value_seen.store(1u, std::memory_order_release);
                       };
                       channel.drain(deliver);
                   });

        c->regions_ready.store(1, std::memory_order_release);

        // The WIRE leg for the SAME topic, exercised in-process while the SHM wake is in
        // flight: a real loopback TCP round-trip of the same payload. The SHM upgrade did
        // NOT suppress the wire path — both deliver the topic's bytes.
        const std::uint32_t wire_got = wire_roundtrip_loopback(k_payload);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while(c->shm_value_seen.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
            io.run_for(std::chrono::milliseconds(20));

        bridge.disarm();

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);

        // BOTH legs delivered the topic's bytes — the same-host SHM ring AND the cross-host
        // wire path coexist on ONE topic (the wire attach was never suppressed).
        REQUIRE(c->shm_value_seen.load(std::memory_order_acquire) == 1u);
        REQUIRE(shm_got == k_payload);
        REQUIRE(wire_got == k_payload);

        ::munmap(c, sizeof(coord));
    }
}
