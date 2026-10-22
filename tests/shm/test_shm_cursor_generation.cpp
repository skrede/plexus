#include "test_shm_ring_core_common.h"

#include "plexus/node_id.h"

#include <cstdint>
#include <optional>

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_cells = 4;
constexpr std::uint64_t k_slot  = 8;

// The waiting word's cross-process encoding, stated apart from the ring so the cases pin it.
constexpr std::uint64_t waiting_word(std::uint32_t generation) noexcept
{
    return (std::uint64_t{1} << 63) | generation;
}

struct generation_ring
{
    generation_ring()
            : control(control_region_bytes(k_cells))
            , slab(slab_region_bytes(k_cells, k_slot))
            , ring()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring, 2) == loan_status::ok);
    }

    std::uint64_t position(std::uint32_t index) noexcept
    {
        return ring.header().cursors[index].position.load();
    }

    backing_region control;
    backing_region slab;
    broadcast_ring ring;
};

std::uint64_t identity_of_fresh_ring(backing_region &control, backing_region &slab)
{
    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring) == loan_status::ok);
    return ring.header().identity.load();
}

}

TEST_CASE("cursor_generation a slot released and re-claimed is not placed by the previous claim's generation", "[shm][cursor_generation]")
{
    generation_ring g;
    const std::optional<std::uint32_t> released = g.ring.claim_cursor(0);
    REQUIRE(released);
    g.ring.unregister_cursor(0);
    const std::optional<std::uint32_t> current = g.ring.claim_cursor(0);
    REQUIRE(current);

    CHECK(*current != *released);
    CHECK_FALSE(g.ring.place_cursor(0, *released, 3));
    CHECK(g.position(0) == k_cursor_idle);
}

TEST_CASE("cursor_generation a placement naming a released claim leaves the slot's new claim waiting for its own placement", "[shm][cursor_generation][!shouldfail]")
{
    generation_ring g;
    const std::optional<std::uint32_t> released = g.ring.claim_cursor(0);
    REQUIRE(released);
    g.ring.unregister_cursor(0);
    const std::optional<std::uint32_t> current = g.ring.claim_cursor(0);
    REQUIRE(current);
    CHECK(*current == *released + 1);

    CHECK_FALSE(g.ring.place_cursor(0, *released, 3));
    CHECK(g.position(0) == waiting_word(*current));
    CHECK(g.ring.place_cursor(0, *current, 5));
    CHECK(g.position(0) == 5);
}

TEST_CASE("cursor_generation a placement naming a waiting reader's slot with a foreign generation is refused", "[shm][cursor_generation]")
{
    generation_ring g;
    const std::optional<std::uint32_t> waiting = g.ring.claim_cursor(1);
    REQUIRE(waiting);

    CHECK_FALSE(g.ring.place_cursor(1, *waiting + 1, 3));
    CHECK_FALSE(g.ring.place_cursor(1, *waiting - 1, 3));
    CHECK(g.position(1) == k_cursor_idle);
}

TEST_CASE("cursor_generation the current claim's generation places the reader", "[shm][cursor_generation]")
{
    generation_ring g;
    REQUIRE(g.ring.claim_cursor(0));
    g.ring.unregister_cursor(0);
    const std::optional<std::uint32_t> current = g.ring.claim_cursor(0);
    REQUIRE(current);

    REQUIRE(g.ring.place_cursor(0, *current, 3));
    CHECK(g.position(0) == 3);
}

TEST_CASE("cursor_generation two rings created under one name carry distinct non-zero identities", "[shm][cursor_generation]")
{
    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    const std::uint64_t first  = identity_of_fresh_ring(control, slab);
    const std::uint64_t second = identity_of_fresh_ring(control, slab);
    CHECK(first != 0);
    CHECK(second != 0);
    CHECK(first != second);
}
