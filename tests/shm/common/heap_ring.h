#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HEAP_RING_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HEAP_RING_H

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/slot_subscriber.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace plexus::testing {

// One ring over cache-line aligned heap regions, for single-process notifier tests.
class heap_ring
{
public:
    heap_ring(std::uint64_t cells, std::uint64_t slot_bytes)
            : m_slab()
            , m_control()
            , m_ring()
            , m_created(plexus::shm::broadcast_ring::create(aligned(m_control, plexus::shm::control_region_bytes(cells)),
                                                            aligned(m_slab, plexus::shm::slab_region_bytes(cells, slot_bytes)), cells, slot_bytes, plexus::node_id{},
                                                            m_ring) == plexus::shm::loan_status::ok)
    {
    }

    bool created() const noexcept
    {
        return m_created;
    }

    plexus::shm::broadcast_ring &ring() noexcept
    {
        return m_ring;
    }

    // Commits one message and wakes the parked waiters, as a producer process would.
    bool commit_one()
    {
        const std::uint32_t payload = 7;
        plexus::shm::broadcast_ring::claim_result claim;
        if(m_ring.claim_with_policy(sizeof(payload), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) != plexus::shm::loan_status::ok)
            return false;
        std::memcpy(claim.slab.data(), &payload, sizeof(payload));
        const bool committed = m_ring.commit(claim.position, sizeof(payload)) == plexus::shm::loan_status::ok;
        plexus::native::notifier_signal(m_ring.header());
        return committed;
    }

private:
    std::vector<std::byte> m_slab;
    std::vector<std::byte> m_control;
    plexus::shm::broadcast_ring m_ring;
    bool m_created;

    static std::span<std::byte> aligned(std::vector<std::byte> &storage, std::size_t bytes)
    {
        storage.assign(bytes + plexus::shm::k_cache_line, std::byte{});
        const auto raw  = reinterpret_cast<std::uintptr_t>(storage.data());
        const auto base = (raw + plexus::shm::k_cache_line - 1) & ~static_cast<std::uintptr_t>(plexus::shm::k_cache_line - 1);
        return {reinterpret_cast<std::byte *>(base), bytes};
    }
};

inline std::uint32_t take_all(plexus::shm::slot_subscriber &sub)
{
    std::uint32_t taken = 0;
    plexus::shm::taken_message msg;
    while(sub.take(msg) == plexus::shm::loan_status::ok)
        ++taken;
    return taken;
}

}

#endif
