// Single-process ring proofs over an anonymous mapped span. The ring takes a
// caller-supplied (control, slab) region pair, so a test maps two heap-backed
// spans and drives claim/commit/consume with no POSIX broker.
#pragma once

#include "support/xproc_harness.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

namespace shm_ring_core_fixture {

using namespace plexus::shm;

// Heap-backed standin for a mapped region: a properly-aligned byte buffer the
// ring places its header/cells (control) or its payload slab over. A real
// backend maps /dev/shm; here a vector with cache-line-aligned storage suffices
// for the single-process logic proof.
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

    std::span<std::byte> span() const noexcept
    {
        return {m_data, m_size};
    }

private:
    std::vector<std::byte> m_storage;
    std::byte *m_data{nullptr};
    std::size_t m_size{0};
};

} // namespace shm_ring_core_fixture
