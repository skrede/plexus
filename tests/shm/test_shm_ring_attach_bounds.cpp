#include "test_shm_ring_core_common.h"

#include "plexus/node_id.h"

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_cells = 16;
constexpr std::uint64_t k_slot  = 64;

plexus::node_id publisher(std::uint8_t last)
{
    plexus::node_id id{};
    id[0]  = std::byte{0x50};
    id[15] = std::byte{last};
    return id;
}

// A header for the geometry whose recorded sizes cover it exactly.
void fill_header(control_header_t &h, std::uint64_t cells, std::uint64_t slot)
{
    h.cell_count        = cells;
    h.mask              = cells - 1;
    h.slot_capacity     = slot;
    h.consumer_capacity = k_max_consumers;
    h.control_bytes     = control_region_bytes(cells);
    h.slab_bytes        = cells * round_up_8(slot);
    h.publisher         = publisher(1);
}

loan_status admits(const control_header_t &h)
{
    return plexus::shm::detail::header_admits(h, h.control_bytes, h.slab_bytes, publisher(1));
}

struct created_ring
{
    created_ring(std::size_t control_extra, std::size_t slab_extra)
            : control(control_region_bytes(k_cells) + control_extra)
            , slab(slab_region_bytes(k_cells, k_slot) + slab_extra)
            , ring()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, publisher(1), ring) == loan_status::ok);
    }

    backing_region control;
    backing_region slab;
    broadcast_ring ring;
};

}

TEST_CASE("ring_attach_bounds a header at exactly the geometry maxima is admitted", "[shm][ring_attach_bounds]")
{
    control_header_t h{};
    fill_header(h, k_max_cells, k_max_slot_capacity);
    REQUIRE(admits(h) == loan_status::ok);
}

TEST_CASE("ring_attach_bounds one step above either geometry maximum is rejected", "[shm][ring_attach_bounds]")
{
    control_header_t cells{};
    fill_header(cells, k_max_cells * 2, k_slot);
    REQUIRE(admits(cells) == loan_status::rejected);

    control_header_t slot{};
    fill_header(slot, k_cells, k_max_slot_capacity + 8);
    REQUIRE(admits(slot) == loan_status::rejected);
}

TEST_CASE("ring_attach_bounds a recorded size below the geometry's need is rejected", "[shm][ring_attach_bounds]")
{
    control_header_t h{};
    fill_header(h, k_cells, k_slot);
    h.slab_bytes -= 8;
    REQUIRE(admits(h) == loan_status::rejected);
}

TEST_CASE("ring_attach_bounds a cell count whose unchecked size product would wrap is rejected", "[shm][ring_attach_bounds]")
{
    created_ring r{0, 0};
    r.ring.header().cell_count = 1ull << 62;
    r.ring.header().mask       = (1ull << 62) - 1;

    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(r.control.span(), r.slab.span(), publisher(1), attacher) == loan_status::rejected);
}

TEST_CASE("ring_attach_bounds a mapped region one byte off its recorded size is rejected", "[shm][ring_attach_bounds]")
{
    created_ring exact{0, 0};
    created_ring padded{1, 1};
    broadcast_ring attacher;

    REQUIRE(broadcast_ring::attach(exact.control.span(), exact.slab.span(), publisher(1), attacher) == loan_status::ok);
    REQUIRE(broadcast_ring::attach(exact.control.span().first(exact.control.span().size() - 1), exact.slab.span(), publisher(1), attacher) == loan_status::rejected);
    REQUIRE(broadcast_ring::attach(exact.control.span(), exact.slab.span().first(exact.slab.span().size() - 1), publisher(1), attacher) == loan_status::rejected);
    REQUIRE(broadcast_ring::attach(padded.control.span().first(padded.control.span().size() - 1), padded.slab.span(), publisher(1), attacher) == loan_status::rejected);
}

TEST_CASE("ring_attach_bounds a ring another publisher owns is rejected", "[shm][ring_attach_bounds]")
{
    created_ring r{0, 0};
    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(r.control.span(), r.slab.span(), publisher(2), attacher) == loan_status::rejected);
    REQUIRE(broadcast_ring::attach(r.control.span(), r.slab.span(), publisher(1), attacher) == loan_status::ok);
}
