#include "test_loan_pool_common.h"

using namespace loan_pool_fixture;

TEST_CASE("loan pool: a pool with an outstanding loan can be moved and the loan stays valid", "[node][loan_pool]")
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
    REQUIRE_FALSE(again.try_borrow(44u)); // capacity-2 pool exhausted, freelist intact across the moves
}

TEST_CASE("loan pool: move assignment re-homes outstanding loans onto the destination pool", "[node][loan_pool]")
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
