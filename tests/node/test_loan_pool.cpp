// The fixed-slot loan pool: borrow/publish lifecycle, alloc-free slot reuse, graceful
// exhaustion, and the publish hand-off that transfers the release obligation to the
// in-flight carrier. A counting type dtor witnesses each release; a borrow on an exhausted
// pool returns an empty loan (never blocks, never grows).

#include "plexus/detail/loan_pool.h"
#include "plexus/io/object_carrier.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

namespace {

using plexus::detail::loan_pool;

// A T whose construction/destruction is observable through static counters, so a slot's
// release (the in-place dtor) is witnessed without reaching into the pool internals.
struct counted
{
    static inline int live = 0;
    static inline int constructed = 0;
    std::uint32_t value;

    explicit counted(std::uint32_t v = 0) : value(v) { ++live; ++constructed; }
    counted(const counted &o) : value(o.value) { ++live; ++constructed; }
    ~counted() { --live; }
};

void reset_counts()
{
    counted::live = 0;
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
    REQUIRE_FALSE(pool.try_borrow(3u));   // exhausted

    a = {};   // drop a — returns its slot
    REQUIRE(counted::live == 1);
    auto c = pool.try_borrow(3u);
    REQUIRE(c);                            // the returned slot is reusable
    REQUIRE(c->value == 3u);
}

TEST_CASE("loan pool: exhaustion returns an empty loan and recovers after a release", "[node][loan_pool]")
{
    reset_counts();
    constexpr std::size_t k_capacity = 3;
    loan_pool<counted> pool{k_capacity};

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
    REQUIRE_FALSE(pool.try_borrow(101u));   // exhausted again
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
