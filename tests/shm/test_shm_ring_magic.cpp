#include "test_shm_ring_core_common.h"

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_previous_ring_magic = 0x50'4c'58'52'4e'47'33'00ull; // "PLXRNG3\0"
constexpr std::uint64_t k_cells               = 16;
constexpr std::uint64_t k_slot                = 64;

loan_status attach_stamped(std::uint64_t magic)
{
    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring creator;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, creator) == loan_status::ok);
    creator.header().magic = magic;

    broadcast_ring attacher;
    return broadcast_ring::attach(control.span(), slab.span(), attacher);
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
