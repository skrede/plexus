#include "test_loan_pool_common.h"

using namespace loan_pool_fixture;

// The object-lane hand-off (carrier_for) transfers a slot's release obligation to an in-flight
// carrier, exactly as a publish does. Destroying the pool while that carrier is outstanding models
// ~publisher -> ~loan_pool -> ~loan_slab running before the posted delivery. The later io::release
// is that delivery. Pre-fix it ran ~T() and returned the node onto freed slab storage (an ASan
// heap-use-after-free); the defer-free control block keeps the storage alive until the last release.

TEST_CASE("loan pool: a slot outstanding past the pool's destruction releases safely", "[node][loan_pool][defer_free]")
{
    reset_counts();

    plexus::io::object_carrier carrier{};
    {
        loan_pool<counted> pool{4};
        auto held = pool.try_borrow(42u);
        REQUIRE(held);
        REQUIRE(counted::live == 1);

        carrier = loan_pool<counted>::carrier_for(held, /*type_tag=*/1u);
        REQUIRE_FALSE(held);
        REQUIRE(carrier.slot != nullptr);
        // pool leaves scope here with the slot still outstanding.
    }

    // The object survives the pool: the deferred control block still owns its storage.
    REQUIRE(counted::live == 1);

    // The posted delivery runs after the pool is gone; ~T() fires exactly once on live storage and
    // the last release frees the control block (ASan/valgrind witness the clean free).
    plexus::io::release(carrier);
    REQUIRE(counted::live == 0);
}

TEST_CASE("loan pool: with several slots outstanding the control block frees only on the last release", "[node][loan_pool][defer_free]")
{
    reset_counts();

    plexus::io::object_carrier c0{};
    plexus::io::object_carrier c1{};
    plexus::io::object_carrier c2{};
    {
        loan_pool<counted> pool{4};
        auto a = pool.try_borrow(1u);
        auto b = pool.try_borrow(2u);
        auto d = pool.try_borrow(3u);
        REQUIRE(a);
        REQUIRE(b);
        REQUIRE(d);
        REQUIRE(counted::live == 3);

        c0 = loan_pool<counted>::carrier_for(a, 1u);
        c1 = loan_pool<counted>::carrier_for(b, 1u);
        c2 = loan_pool<counted>::carrier_for(d, 1u);
        // pool destroyed with three slots outstanding.
    }
    REQUIRE(counted::live == 3);

    // Each release touches the still-live control block; freeing it before the last would UAF the
    // remaining releases. Every ~T() fires exactly once and the count walks cleanly to zero.
    plexus::io::release(c0);
    REQUIRE(counted::live == 2);
    plexus::io::release(c1);
    REQUIRE(counted::live == 1);
    plexus::io::release(c2);
    REQUIRE(counted::live == 0);
}
