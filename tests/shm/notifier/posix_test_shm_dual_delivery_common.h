#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_POSIX_TEST_SHM_DUAL_DELIVERY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_POSIX_TEST_SHM_DUAL_DELIVERY_COMMON_H

#include "region_names.h"

#include "plexus/native/region_handle.h"
#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/broadcast_ring.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <asio/read.hpp>
#include <asio/write.hpp>
#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>

#include <sys/mman.h>

#include <atomic>
#include <string>
#include <cstdint>
#include <cstring>

namespace dual_delivery_fixture {

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0};
    std::atomic<std::uint32_t> shm_value_seen{0};
};

inline coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

inline std::string control_name(const std::string &fqn)
{
    return plexus::testing::region_name(fqn, plexus::testing::test_publisher());
}
inline std::string slab_name(const std::string &fqn)
{
    return plexus::testing::region_name(fqn, plexus::testing::test_publisher()) + ".s";
}

constexpr std::uint32_t k_payload = 0xD04D11EFu;

// The producer child: attach the parent's SHM regions, publish ONE value, wake the
// parked slot (the SHM cross-process wake the parent's bridge is parked on).
inline bool produce_shm(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok || broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;
    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), plexus::testing::test_publisher(), ring) != pio::loan_status::ok)
        return false;
    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) != pio::loan_status::ok)
        return false;
    std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
    if(ring.commit(claim.position, sizeof(k_payload)) != pio::loan_status::ok)
        return false;
    plexus::native::notifier_signal(ring.header());
    return true;
}

// The wire leg over real TCP loopback: a server socket accepts one client; the client
// sends the topic's payload, the server reads it back. The SAME fqn keys both legs; the
// wire path is exercised independently of (and concurrently with) the SHM upgrade,
// proving the upgrade did not suppress it. Returns the bytes the server received.
inline std::uint32_t wire_roundtrip_loopback(std::uint32_t payload)
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acceptor(io, ::asio::ip::tcp::endpoint(::asio::ip::tcp::v4(), 0));
    const auto port = acceptor.local_endpoint().port();

    ::asio::ip::tcp::socket client(io);
    client.connect(::asio::ip::tcp::endpoint(::asio::ip::make_address("127.0.0.1"), port));
    ::asio::ip::tcp::socket server = acceptor.accept();

    ::asio::write(client, ::asio::buffer(&payload, sizeof(payload)));
    std::uint32_t got = 0;
    ::asio::read(server, ::asio::buffer(&got, sizeof(got)));
    return got;
}

}

#endif
