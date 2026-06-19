// The fixed-slot loan pool: borrow/publish lifecycle, alloc-free slot reuse, graceful
// exhaustion, and the publish hand-off that transfers the release obligation to the
// in-flight carrier. A counting type dtor witnesses each release; a borrow on an exhausted
// pool returns an empty loan (never blocks, never grows).

#include "plexus/detail/loan_pool.h"
#include "plexus/io/object_carrier.h"

#include <catch2/catch_test_macros.hpp>

#include <utility>
#include <cstdint>

namespace {

using plexus::detail::loan_pool;

// A T whose construction/destruction is observable through static counters, so a slot's
// release (the in-place dtor) is witnessed without reaching into the pool internals.
struct counted
{
    static inline int live        = 0;
    static inline int constructed = 0;
    std::uint32_t     value;

    explicit counted(std::uint32_t v = 0)
            : value(v)
    {
        ++live;
        ++constructed;
    }
    counted(const counted &o)
            : value(o.value)
    {
        ++live;
        ++constructed;
    }
    ~counted() { --live; }
};

void reset_counts()
{
    counted::live        = 0;
    counted::constructed = 0;
}

}

TEST_CASE("loan pool: a borrowed object is live until its loan releases", "[node][loan_pool]")
{
    reset_counts();
    loan_pool<counted> pool{4};
    {
        auto held = pool.try_borrow(42u);
        REQUIRE(held);
        REQUIRE(held->value == 42u);
        REQUIRE(counted::live == 1);
    }
    REQUIRE(counted::live == 0);
}

TEST_CASE("loan pool: publishing transfers the release to the carrier", "[node][loan_pool]")
{
    reset_counts();
    loan_pool<counted> pool{4};

    auto held = pool.try_borrow(7u);
    REQUIRE(counted::live == 1);

    // The publish hand-off: carrier_for surrenders the slot to a carrier (refs already 1
    // from the borrow). The loan is left empty, so its destructor releases nothing.
    auto carrier = loan_pool<counted>::carrier_for(held, /*type_tag=*/1u);
    REQUIRE_FALSE(held);
    REQUIRE(carrier.slot != nullptr);
    REQUIRE(carrier.native_key == &plexus::io::detail::type_key<counted>);
    REQUIRE(counted::live == 1);

    // The carrier's single reference released destroys the object in place and returns
    // the slot — exactly the forwarder's post-fan release.
    plexus::io::release(carrier);
    REQUIRE(counted::live == 0);
}

TEST_CASE("loan pool: a never-published loan returns its slot on destruction", "[node][loan_pool]")
{
    reset_counts();
    loan_pool<counted> pool{2};

    // Drain to capacity, then drop one and re-borrow — the dropped slot must be reusable
    // (its destructor returned it), proving no slot leaks on the un-published path.
    auto a = pool.try_borrow(1u);
    auto b = pool.try_borrow(2u);
    REQUIRE(a);
    REQUIRE(b);
    REQUIRE_FALSE(pool.try_borrow(3u)); // exhausted

    a = {}; // drop a — returns its slot
    REQUIRE(counted::live == 1);
    auto c = pool.try_borrow(3u);
    REQUIRE(c); // the returned slot is reusable
    REQUIRE(c->value == 3u);
}

TEST_CASE("loan pool: exhaustion returns an empty loan and recovers after a release",
          "[node][loan_pool]")
{
    reset_counts();
    constexpr std::size_t k_capacity = 3;
    loan_pool<counted>    pool{k_capacity};

    plexus::detail::loan<counted> held[k_capacity];
    for(std::size_t i = 0; i < k_capacity; ++i)
    {
        held[i] = pool.try_borrow(static_cast<std::uint32_t>(i));
        REQUIRE(held[i]);
    }
    REQUIRE(counted::live == static_cast<int>(k_capacity));

    // N+1th borrow at capacity: empty, no growth, no allocation, no block.
    REQUIRE_FALSE(pool.try_borrow(99u));

    // Release one and the pool recovers exactly one slot.
    held[0] = {};
    REQUIRE(counted::live == static_cast<int>(k_capacity) - 1);
    auto recovered = pool.try_borrow(100u);
    REQUIRE(recovered);
    REQUIRE(recovered->value == 100u);
    REQUIRE_FALSE(pool.try_borrow(101u)); // exhausted again
}

TEST_CASE("loan pool: a pool with an outstanding loan can be moved and the loan stays valid",
          "[node][loan_pool]")
{
    reset_counts();

    // Borrow while the pool lives in `src`, then MOVE the pool out of `src`. The loan
    // holds &n->control by address (unchanged by the move) but each slot's owner is
    // re-stamped to `moved`, so the loan's eventual release routes to the LIVE pool's
    // freelist — never the moved-from one.
    loan_pool<counted> moved = []
    {
        loan_pool<counted> src{2};
        return src; // the move-ctor steals the slot array and re-stamps owner
    }();

    auto held = moved.try_borrow(11u);
    REQUIRE(held);
    REQUIRE(held->value == 11u);
    REQUIRE(counted::live == 1);

    // Move the pool AGAIN with the loan outstanding, then deref/release through the loan.
    loan_pool<counted> again = std::move(moved);
    REQUIRE(held);
    REQUIRE(held->value == 11u);

    held = {}; // release routes to `again`'s freelist
    REQUIRE(counted::live == 0);

    // The released slot returns to the moved pool and re-borrows cleanly (no leak, no UAF).
    auto reborrow = again.try_borrow(22u);
    REQUIRE(reborrow);
    REQUIRE(reborrow->value == 22u);
    auto second = again.try_borrow(33u);
    REQUIRE(second);
    REQUIRE_FALSE(
            again.try_borrow(44u)); // capacity-2 pool exhausted, freelist intact across the moves
}

TEST_CASE("loan pool: move assignment re-homes outstanding loans onto the destination pool",
          "[node][loan_pool]")
{
    reset_counts();
    loan_pool<counted> dst{1};
    loan_pool<counted> src{2};

    auto held = src.try_borrow(5u);
    REQUIRE(held);
    REQUIRE(counted::live == 1);

    dst = std::move(src); // dst's own slot freed; src's slots + freelist stolen, owner re-stamped
    REQUIRE(held);
    REQUIRE(held->value == 5u);

    held = {}; // routes to dst's freelist (the re-stamped owner)
    REQUIRE(counted::live == 0);

    auto a = dst.try_borrow(1u);
    auto b = dst.try_borrow(2u);
    REQUIRE(a);
    REQUIRE(b); // capacity-2 (the moved-in pool), not dst's original 1
    REQUIRE_FALSE(dst.try_borrow(3u));
}

TEST_CASE("loan pool: slots reuse without reconstructing the backing array", "[node][loan_pool]")
{
    reset_counts();
    loan_pool<counted> pool{2};

    // A long churn loop borrows and releases far more times than the capacity. The pool
    // allocated its slots ONCE at construction; every borrow here reuses a freed slot, so
    // the live count never exceeds capacity and the constructed count climbs with churn
    // (each borrow placement-news a fresh object into a reused slot).
    for(int i = 0; i < 1000; ++i)
    {
        auto held = pool.try_borrow(static_cast<std::uint32_t>(i));
        REQUIRE(held);
        REQUIRE(counted::live <= 2);
    }
    REQUIRE(counted::live == 0);
    REQUIRE(counted::constructed == 1000);
}
