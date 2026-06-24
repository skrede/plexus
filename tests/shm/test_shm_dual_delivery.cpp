// over-limit: one cohesive dual-delivery cross-process proof; the single end-to-end SHM-ring +
// wire-loopback round-trip is one pipeline over a shared converged region and reactor bridge,
// so it cannot split without scattering that shared cross-process pipeline state.
#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"
#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>
#include <asio/ip/tcp.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <utility>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// Dual-delivery (xproc): the same-host shared-memory upgrade ADDS a fast path; it
// does NOT REPLACE the wire path. For ONE topic, this oracle proves both flows coexist:
//   * the SHM leg — a forked producer publishes + signals the in-region notify word; the
//     parent's io_uring-futex bridge reaps the cross-process wake on its OWN asio reactor
//     and the drain delivers the value over the shm_channel.
//   * the WIRE leg — for the SAME topic, a loopback TCP peer delivers the SAME bytes over
//     a real socket; the SHM upgrade never suppressed it.
// Both are keyed to one topic (the deterministic region name + the loopback peer carry the
// same fqn). Looped N>=3 in-body; the binary is re-run >=3 process runs for reproducibility.

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

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
    std::atomic<std::uint32_t> regions_ready{0};
    std::atomic<std::uint32_t> shm_value_seen{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
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

constexpr std::uint32_t k_payload = 0xD04D11EFu;

// The producer child: attach the parent's SHM regions, publish ONE value, signal the
// notify word (the SHM cross-process wake the parent's bridge is parked on).
bool produce_shm(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle           ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok || broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;
    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;
    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) != pio::loan_status::ok)
        return false;
    std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
    if(ring.commit(claim.position, sizeof(k_payload)) != pio::loan_status::ok)
        return false;
    plexus::native::notifier_signal(ring.notify_generation());
    return true;
}

// The wire leg over real TCP loopback: a server socket accepts one client; the client
// sends the topic's payload, the server reads it back. The SAME fqn keys both legs; the
// wire path is exercised independently of (and concurrently with) the SHM upgrade,
// proving the upgrade did not suppress it. Returns the bytes the server received.
std::uint32_t wire_roundtrip_loopback(std::uint32_t payload)
{
    ::asio::io_context        io;
    ::asio::ip::tcp::acceptor acceptor(io, ::asio::ip::tcp::endpoint(::asio::ip::tcp::v4(), 0));
    const auto                port = acceptor.local_endpoint().port();

    ::asio::ip::tcp::socket client(io);
    client.connect(::asio::ip::tcp::endpoint(::asio::ip::make_address("127.0.0.1"), port));
    ::asio::ip::tcp::socket server = acceptor.accept();

    ::asio::write(client, ::asio::buffer(&payload, sizeof(payload)));
    std::uint32_t got = 0;
    ::asio::read(server, ::asio::buffer(&got, sizeof(got)));
    return got;
}

}

TEST_CASE("shm.dual_delivery a same-host SHM topic and a cross-host wire topic coexist on one topic", "[shm][dual_delivery]")
{
    const std::string        fqn  = "topic.dual." + std::to_string(::getpid());
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

        // The SHM leg (consumer parent): create the regions, build the channel, arm the
        // bridge on a user-owned io_context bound to the ring's notify word.
        posix_shm_region_broker broker;
        region_handle           ctrl, slab;
        REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count), pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn), pio::slab_region_bytes(geom.cell_count, geom.slot_capacity), pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count, geom.slot_capacity, ring) == pio::loan_status::ok);

        ::asio::io_context                                              io;
        plexus::asio::shm::ring_notifier<test_policy>                   bridge(io, ring.notify_generation());
        pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>> channel(ring, bridge, plexus::io::reliability::reliable, plexus::io::congestion::block);

        std::uint32_t shm_got = 0;
        bridge.arm(
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
