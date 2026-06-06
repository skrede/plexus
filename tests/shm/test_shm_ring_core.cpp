#include "support/xproc_harness.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

// Single-process ring proofs over an anonymous mapped span. The ring takes a
// caller-supplied (control, slab) region pair, so a test maps two heap-backed
// spans and drives claim/commit/consume with no POSIX broker. ring_sizing
// exercises ring_geometry_for + the slab-ceiling fast-fail; ring_core round-trips
// a payload looped N>=100.

using namespace plexus::io::shm;

namespace {

// Heap-backed standin for a mapped region: a properly-aligned byte buffer the
// ring places its header/cells (control) or its payload slab over. A real
// backend maps /dev/shm; here a vector with cache-line-aligned storage suffices
// for the single-process logic proof.
struct backing_region
{
    explicit backing_region(std::size_t bytes)
        : m_storage(bytes + k_cache_line)
    {
        auto base = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + (k_cache_line - 1)) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data = reinterpret_cast<std::byte *>(aligned);
        m_size = bytes;
    }

    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte *m_data{nullptr};
    std::size_t m_size{0};
};

}

TEST_CASE("ring_sizing: ring_geometry_for trades depth for width under a ceiling", "[shm][ring_sizing]")
{
    // Default deep-but-narrow ring for an unset / small declaration.
    const ring_geometry small = ring_geometry_for(std::nullopt);
    REQUIRE(small.cell_count == 256);
    REQUIRE(small.slot_capacity == 4096);
    REQUIRE(small.cell_count >= k_max_consumers);

    // The depth steps down as the slot grows; the slab stays under the ceiling.
    constexpr std::uint64_t k_max_ring_slab_bytes = 16ull * 1024 * 1024;
    for (std::uint32_t payload : {64u, 4096u, 40000u, 70000u, 200000u, 600000u, 1048576u})
    {
        const ring_geometry g = ring_geometry_for(payload);
        REQUIRE(g.slot_capacity >= payload);
        REQUIRE(g.slot_capacity % 8 == 0);
        REQUIRE((g.cell_count & (g.cell_count - 1)) == 0); // power of two
        REQUIRE(g.cell_count >= k_max_consumers);
        REQUIRE(g.cell_count * g.slot_capacity <= k_max_ring_slab_bytes);
    }
}

TEST_CASE("ring_layout: the cross-process structs are lock-free standard-layout", "[shm][ring_sizing]")
{
    STATIC_REQUIRE(std::atomic<std::uint64_t>::is_always_lock_free);
    STATIC_REQUIRE(std::atomic<std::uint32_t>::is_always_lock_free);
    STATIC_REQUIRE(std::is_standard_layout_v<cell_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<cursor_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<control_header_t>);
    REQUIRE(k_cache_line == 64);
}

TEST_CASE("ring_core: claim/commit/consume round-trips a payload single-process", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 64;
    constexpr std::uint64_t k_slot = 256;
    constexpr int k_iterations = 200;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
            loan_status::ok);

    std::uint32_t cursor_index = 0;
    REQUIRE(ring.register_cursor(cursor_index) == loan_status::ok);
    std::uint64_t cursor = ring.tail_position();
    ring.publish_cursor(cursor_index, cursor);

    // Loop well past one lap (200 > 64 cells): the reliable claim gates on this
    // single registered cursor, which advances each iteration, so reclamation
    // keeps pace and the round-trip never congests. Each consumed value must read
    // back exactly the bytes the producer wrote.
    for (int i = 0; i < k_iterations; ++i)
    {
        const std::uint32_t value = 0xC0DE0000u | static_cast<std::uint32_t>(i);

        broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(value), plexus::io::reliability::reliable,
                                       plexus::io::congestion::block, claim) == loan_status::ok);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);

        broadcast_ring::consume_result consumed;
        REQUIRE(ring.consume(cursor, consumed) == loan_status::ok);
        REQUIRE(consumed.slab.size() == sizeof(value));

        std::uint32_t read = 0;
        std::memcpy(&read, consumed.slab.data(), sizeof(read));
        REQUIRE(read == value);

        ++cursor;
        ring.publish_cursor(cursor_index, cursor);
    }

    ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_core: attach re-reads and bounds-checks the header", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 32;
    constexpr std::uint64_t k_slot = 128;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring creator;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, creator) ==
            loan_status::ok);

    // A second ring attaches the SAME backing spans and re-reads the geometry.
    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(control.span(), slab.span(), attacher) == loan_status::ok);
    REQUIRE(attacher.cell_count() == k_cells);
    REQUIRE(attacher.slot_capacity() == k_slot);

    // A foreign / unmapped control region (no magic) is rejected.
    backing_region foreign_control(control_region_bytes(k_cells));
    broadcast_ring rejected;
    REQUIRE(broadcast_ring::attach(foreign_control.span(), slab.span(), rejected) ==
            loan_status::rejected);
}
