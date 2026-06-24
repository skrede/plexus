// Backpressure proofs over an anonymous mapped span: best_effort overwrite skips
// a pinned slot (a held take is not stomped); a full-lap-pinned ring returns
// congested; reliable claim gates on the slowest registered cursor (lossless).
#pragma once

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace shm_ring_backpressure_fixture {

using namespace plexus::shm;

struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + (k_cache_line - 1)) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data       = reinterpret_cast<std::byte *>(aligned);
        m_size       = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

struct fixture
{
    static constexpr std::uint64_t k_cells = 16;
    static constexpr std::uint64_t k_slot  = 64;

    backing_region control{control_region_bytes(k_cells)};
    backing_region slab{slab_region_bytes(k_cells, k_slot)};
    broadcast_ring ring;

    fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
                loan_status::ok);
    }

    void put(plexus::io::reliability rel, std::uint32_t value)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(value), rel, plexus::io::congestion::drop_newest,
                                       claim) == loan_status::ok);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);
    }
};

} // namespace shm_ring_backpressure_fixture
