#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_RING_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_RING_H

#include "test_node_xproc_shm_common.h"

#include "support/region_names.h"

#include "plexus/native/region_handle.h"
#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/broadcast_ring.h"

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace node_xproc_shm_fixture {

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

inline std::string control_name(const std::string &fqn)
{
    return plexus::testing::region_name(fqn, make_id(0x52));
}

inline std::string slab_name(const std::string &fqn)
{
    return plexus::testing::region_name(fqn, make_id(0x52)) + ".s";
}

// The control region maps iff the facade auto-engaged the companion ring, so its attach status is the medium.
inline bool region_maps(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle ctrl;
    return broker.attach(control_name(fqn), ctrl) == pio::region_status::ok;
}

// The ring carries the framed message, whose framing a raw consumer cannot re-derive, so the run's payload is searched
// for inside the delivered slab.
inline bool contains(std::span<const std::byte> hay, std::span<const std::byte> needle)
{
    if(needle.empty() || needle.size() > hay.size())
        return false;
    const auto *h = reinterpret_cast<const unsigned char *>(hay.data());
    const auto *n = reinterpret_cast<const unsigned char *>(needle.data());
    return std::search(h, h + hay.size(), n, n + needle.size()) != h + hay.size();
}

inline bool attach_ring(const std::string &fqn, std::chrono::steady_clock::time_point deadline, region_handle &ctrl, region_handle &slab, pio::broadcast_ring &ring)
{
    posix_shm_region_broker broker;
    while(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok || broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
    return pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), make_id(0x52), ring) == pio::loan_status::ok;
}

// pump keeps the subscriber node answering its publisher's offer meanwhile, since the publisher routes over the ring
// only after that answer.
template<typename Pump>
bool ring_carries(const std::string &fqn, std::span<const std::byte> needle, std::chrono::seconds bound, Pump pump)
{
    region_handle ctrl, slab;
    pio::broadcast_ring ring;
    const auto deadline = std::chrono::steady_clock::now() + bound;
    if(!attach_ring(fqn, deadline, ctrl, slab, ring))
        return false;
    std::uint64_t cursor = 0;
    while(std::chrono::steady_clock::now() < deadline)
    {
        pump();
        pio::broadcast_ring::consume_result out;
        const auto st = ring.consume(cursor, out);
        if(st == pio::loan_status::ok && contains(out.slab, needle))
            return true;
        if(st == pio::loan_status::ok || st == pio::loan_status::congested)
            ++cursor;
        else if(st == pio::loan_status::lagged)
            cursor = out.position; // lapped past a full ring: re-sync to the oldest valid slot
    }
    return false;
}

}

#endif
