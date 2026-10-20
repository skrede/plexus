#include "test_shm_ring_core_common.h"

#include "support/noop_notifier.h"

#include "plexus/shm/ring_cells.h"
#include "plexus/shm/shm_channel.h"

#include "plexus/node_id.h"

#include <new>
#include <atomic>
#include <cstddef>
#include <cstdint>

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_cells = 4;
constexpr std::uint64_t k_slot  = 8;

// The park line follows the cursor array and the notify line, so a cursor index one past the array reads the park
// words as a cursor: park[0] and park[1] as its position, park[2] as its active word.
constexpr std::uint32_t k_park_alias = k_max_consumers + 1;

struct tampered_ring
{
    tampered_ring()
            : control(control_region_bytes(k_cells))
            , slab(slab_region_bytes(k_cells, k_slot))
            , ring()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring, 1) == loan_status::ok);
    }

    loan_status claim() noexcept
    {
        broadcast_ring::claim_result claimed;
        const loan_status st = ring.claim_with_policy(1, plexus::io::reliability::reliable, plexus::io::congestion::block, claimed);
        if(st == loan_status::ok)
            ring.commit(claimed.position, 1);
        return st;
    }

    cell_t &first_cell() const noexcept
    {
        return *std::launder(reinterpret_cast<cell_t *>(control.span().data() + sizeof(control_header_t)));
    }

    backing_region control;
    backing_region slab;
    broadcast_ring ring;
};

}

TEST_CASE("ring_tampered a high-water a peer raised past the cursor array gates no claim", "[shm][ring_tampered]")
{
    static_assert(offsetof(control_header_t, park) == offsetof(control_header_t, cursors) + k_park_alias * sizeof(cursor_t));
    tampered_ring t;
    for(std::uint64_t i = 0; i < k_cells; ++i)
        REQUIRE(t.claim() == loan_status::ok);
    t.ring.header().park[2].store(k_park_parked);
    t.ring.header().high_water.store(k_park_alias + 1);

    CHECK(t.ring.registered_high_water() <= k_max_consumers);
    CHECK(t.ring.slowest_consumer_position() == k_cursor_idle);
    REQUIRE(t.claim() == loan_status::ok);
}

TEST_CASE("ring_tampered a reader index past the cursor array is not placed though a peer raised the capacity", "[shm][ring_tampered]")
{
    tampered_ring t;
    REQUIRE(t.claim() == loan_status::ok);
    t.ring.header().park[0].store(UINT32_MAX);
    t.ring.header().park[1].store(UINT32_MAX);
    t.ring.header().park[2].store(k_park_parked);
    t.ring.header().consumer_capacity = 2 * k_max_consumers;

    plexus::testing::noop_notifier notifier;
    shm_channel<plexus::testing::noop_notifier> channel{t.ring, notifier, plexus::io::reliability::reliable, plexus::io::congestion::block};
    CHECK_FALSE(channel.place_reader(k_park_alias));
    REQUIRE(t.ring.header().park[0].load() == UINT32_MAX);
}

TEST_CASE("ring_tampered a sequence a lap ahead with no tail past the cursor is not a lag", "[shm][ring_tampered]")
{
    tampered_ring t;
    t.first_cell().sequence.store(1 + k_cells);

    broadcast_ring::consume_result out;
    REQUIRE(t.ring.consume(0, out) == loan_status::empty);
}
