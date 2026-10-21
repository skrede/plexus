#include "test_shm_ring_core_common.h"

#include "plexus/shm/waiter_park.h"

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_previous_ring_magic = 0x50'4c'58'52'4e'47'35'00ull; // "PLXRNG5\0"
constexpr std::uint64_t k_cells               = 16;
constexpr std::uint64_t k_slot                = 64;

loan_status attach_stamped(std::uint64_t magic)
{
    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring creator;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, creator) == loan_status::ok);
    creator.header().magic.store(magic);

    broadcast_ring attacher;
    return broadcast_ring::attach(control.span(), slab.span(), plexus::node_id{}, attacher);
}

}

TEST_CASE("ring_magic a region stamped with the previous layout magic fails closed", "[shm][ring_magic][!shouldfail]")
{
    REQUIRE(attach_stamped(k_previous_ring_magic) == loan_status::rejected);
}

TEST_CASE("ring_magic a region stamped with the current magic attaches", "[shm][ring_magic]")
{
    REQUIRE(attach_stamped(k_ring_magic) == loan_status::ok);
}

TEST_CASE("ring_magic a region whose creator has not stamped it yet is not refused as foreign", "[shm][ring_magic]")
{
    REQUIRE(attach_stamped(0) == loan_status::unstamped);
}

TEST_CASE("ring_magic every slot's park word sits on one cache line apart from the cursors", "[shm][ring_magic]")
{
    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));
    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring) == loan_status::ok);

    const auto *base  = reinterpret_cast<const std::byte *>(&ring.header());
    const auto *first = reinterpret_cast<const std::byte *>(&plexus::shm::park_word(ring.header(), 0));
    const auto *last  = reinterpret_cast<const std::byte *>(&plexus::shm::park_word(ring.header(), k_max_consumers - 1));
    const auto *after = reinterpret_cast<const std::byte *>(&ring.header().cursors[k_max_consumers - 1]) + sizeof(cursor_t);

    REQUIRE((first - base) % static_cast<std::ptrdiff_t>(k_cache_line) == 0);
    REQUIRE(last - first == static_cast<std::ptrdiff_t>(k_cache_line - sizeof(std::uint32_t)));
    REQUIRE(first >= after);
    for(std::uint32_t slot = 0; slot < k_max_consumers; ++slot)
        REQUIRE(plexus::shm::park_word(ring.header(), slot).load() == k_park_empty);
}
