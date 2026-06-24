// Backpressure + observable congestion: a best_effort ring with every slot pinned
// returns congested off send() (OBSERVABLE, never a silent drop). A reliable
// producer with a lagging consumer blocks losslessly -- every value arrives in
// order, none skipped. And the reliable blocking spin/yield path allocates nothing
// across N sends (the alloc-counter idiom).
#pragma once

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <vector>

namespace shm_congested_fixture {

using namespace plexus::shm;

// A no-op notifier satisfying the seam (this TU exercises the loan/publish gate,
// not the wakeup; the channel still requires a notifier reference).
struct null_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(notifier<null_notifier>, "null_notifier must satisfy the notifier seam");

struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + k_cache_line - 1) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data       = reinterpret_cast<std::byte *>(aligned);
        m_size       = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

template<std::uint64_t Cells, std::uint64_t Slot>
struct ring_fixture
{
    backing_region control{control_region_bytes(Cells)};
    backing_region slab{slab_region_bytes(Cells, Slot)};
    broadcast_ring ring;
    null_notifier  notify;

    ring_fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), Cells, Slot, ring) ==
                loan_status::ok);
    }
};

inline std::uint32_t as_u32(std::span<const std::byte> b)
{
    std::uint32_t v = 0;
    std::memcpy(&v, b.data(), sizeof(v));
    return v;
}

} // namespace shm_congested_fixture
