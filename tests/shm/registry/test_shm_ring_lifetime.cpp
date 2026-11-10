#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <latch>
#include <thread>
#include <vector>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using values = std::vector<std::uint32_t>;

std::vector<acquire_result> race_for(ring_holder &a, ring_holder &b)
{
    std::vector<acquire_result> got(2, acquire_result::failed);
    std::latch start{2};
    std::thread first(
            [&]
            {
                start.arrive_and_wait();
                got[0] = a.acquire(ring_role::producer);
            });
    std::thread second(
            [&]
            {
                start.arrive_and_wait();
                got[1] = b.acquire(ring_role::producer);
            });
    first.join();
    second.join();
    return got;
}

}

TEST_CASE("ring_lifetime the last holder of either role removes the ring's names", "[shm][ring_lifetime]")
{
    const bool producer_last = GENERATE(true, false);
    wake_bus bus;
    ring_view view{lifetime_fqn("last_out")};
    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);

    if(producer_last)
        reader.release(ring_role::consumer);
    else
        publisher.release(ring_role::producer);
    REQUIRE(view.named());
    if(producer_last)
        publisher.release(ring_role::producer);
    else
        reader.release(ring_role::consumer);
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_lifetime a restarted publisher adopts its ring and the reader keeps receiving in order", "[shm][ring_lifetime]")
{
    const std::uint32_t capacity = GENERATE(0u, 1u);
    wake_bus bus;
    ring_view view{lifetime_fqn("adopt")};
    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer, 64, capacity) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(1) == plexus::shm::loan_status::ok);
    publisher.release(ring_role::producer);
    REQUIRE(view.named());

    ring_holder restarted{bus, view.fqn};
    REQUIRE(restarted.acquire(ring_role::producer, 64, capacity) == acquire_result::attached);
    REQUIRE(restarted.acquire(ring_role::producer, 64, capacity) == acquire_result::attached);
    REQUIRE(restarted.send(2) == plexus::shm::loan_status::ok);
    REQUIRE(restarted.send(3) == plexus::shm::loan_status::ok);
    CHECK(reader.received() == values{1, 2, 3});

    for(std::uint32_t released = 0; released < 3; ++released)
        restarted.release(ring_role::producer);
    REQUIRE(view.named());
    reader.release(ring_role::consumer);
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_lifetime a second live publisher is refused as creator_alive and the ring stays with the first", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("alive")};
    ring_holder publisher{bus, view.fqn};
    ring_holder second{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);

    REQUIRE(second.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(second.last_bound() == acquire_bound::creator_alive);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(7) == plexus::shm::loan_status::ok);
    CHECK(reader.received() == values{7});
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
}

TEST_CASE("ring_lifetime of two restarted publishers racing for one ring exactly one adopts it", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("race")};
    ring_holder reader{bus, view.fqn};
    {
        ring_holder publisher{bus, view.fqn};
        REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
        REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
        publisher.release(ring_role::producer);
    }
    for(std::uint32_t round = 0; round < 50; ++round)
    {
        ring_holder a{bus, view.fqn};
        ring_holder b{bus, view.fqn};
        const std::vector<acquire_result> got = race_for(a, b);
        REQUIRE((got[0] == acquire_result::attached) != (got[1] == acquire_result::attached));
        ring_holder &loser  = got[0] == acquire_result::attached ? b : a;
        ring_holder &winner = got[0] == acquire_result::attached ? a : b;
        CHECK(loser.last_bound() == acquire_bound::creator_alive);
        REQUIRE(winner.send(round) == plexus::shm::loan_status::ok);
        winner.release(ring_role::producer);
    }
    CHECK(reader.received().size() == 50);
    reader.release(ring_role::consumer);
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_lifetime a joiner meets a ring its last holder is retiring as transient and then mints a fresh one", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retiring")};
    ring_holder publisher{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(view.map());
    REQUIRE(view.control.hold_slot(plexus::shm::cursor_slot(0)) == plexus::shm::slot_hold::held);
    publisher.release(ring_role::producer);
    REQUIRE(view.named());
    view.control.release_slot(plexus::shm::cursor_slot(0));
    REQUIRE(view.control.hold_all_slots() == plexus::shm::slot_hold::held);
    view.ring.header().magic.store(plexus::shm::k_ring_retired, std::memory_order_release);

    ring_holder newcomer{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(newcomer.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(newcomer.last_bound() == acquire_bound::creator_alive);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(reader.last_bound() == acquire_bound::transient_join);

    view.slab.unlink_name();
    view.control.unlink_name();
    view.control.release_all_slots();
#if defined(_WIN32)
    // A Windows section keeps its name until its last view closes, so a joiner before the retirer's unmap meets it retired.
    REQUIRE(newcomer.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(newcomer.last_bound() == acquire_bound::transient_join);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(reader.last_bound() == acquire_bound::transient_join);
#endif
    view.unmap();
    REQUIRE(newcomer.acquire(ring_role::producer) == acquire_result::created);
    newcomer.release(ring_role::producer);
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_lifetime a reader that died before it read leaves its cursor to the next reader", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("dead_reader")};
    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer, 64, 1) == acquire_result::created);
    REQUIRE(view.map());
    REQUIRE(view.control.hold_slot(plexus::shm::cursor_slot(0)) == plexus::shm::slot_hold::held);
    REQUIRE(view.ring.claim_cursor(0));
    view.control.release_slot(plexus::shm::cursor_slot(0));

    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(5) == plexus::shm::loan_status::ok);
    CHECK(reader.received() == values{5});
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
}
