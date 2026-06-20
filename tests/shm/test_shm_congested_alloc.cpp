#include "test_shm_congested_common.h"

#include "support/alloc_counter.h"

using namespace shm_congested_fixture;

TEST_CASE("shm.backpressure the reliable blocking spin allocates nothing", "[shm][backpressure]")
{
    constexpr std::uint64_t   k_cells = 16;
    constexpr int             k_total = 2000;
    ring_fixture<k_cells, 64> f;

    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable,
                                       plexus::io::congestion::block);

    // The deliver callback counts drained messages into an atomic (NO heap in its
    // body). It is constructed BEFORE the snapshot, so the one-time
    // move_only_function construction never falls inside the measured window.
    std::atomic<int>                       drained{0};
    shm_channel<null_notifier>::deliver_fn deliver = [&](plexus::wire_bytes<shm_slot_owner>)
    { drained.fetch_add(1, std::memory_order_release); };

    // Bring the consumer thread fully up (its stack + any one-time allocation lands)
    // and barrier on a flag BEFORE snapshotting the allocation counter, so only the
    // steady-state send/drain loops fall inside the measured window.
    std::atomic<bool> go{false};
    std::thread       reader(
            [&]
            {
                while(!go.load(std::memory_order_acquire))
                    std::this_thread::yield();
                while(drained.load(std::memory_order_acquire) < k_total)
                {
                    channel.drain(deliver);
                    if(drained.load(std::memory_order_acquire) < k_total)
                        std::this_thread::yield();
                }
            });

    const std::size_t before = plexus::testing::alloc_count();
    go.store(true, std::memory_order_release);

    for(int i = 0; i < k_total; ++i)
    {
        const std::uint32_t v = static_cast<std::uint32_t>(i);
        std::byte           bytes[sizeof(v)];
        std::memcpy(bytes, &v, sizeof(v));
        REQUIRE(channel.send(std::span<const std::byte>(bytes, sizeof(bytes))) == loan_status::ok);
    }
    reader.join();
    const std::size_t after = plexus::testing::alloc_count();

    // The steady-state send loop (incl. the reliable spin/yield back-pressure path)
    // and the drain loop allocated NOTHING: no kernel object, no heap -- the
    // determinism the safety/drone use requires.
    REQUIRE(after - before == 0);
}

TEST_CASE("shm.congested a reliable claim on a pinned cell returns congested past the spin budget",
          "[shm][congested][spin_budget]")
{
    // A reliable claim whose target cell is still held by a live take() pin used to
    // spin the producer core indefinitely. It now relaxes the core a bounded number
    // of iterations and returns congested (the slow consumer holds a legit loan --
    // surface it, never pin 100% core). We pin the cell the next contiguous claim
    // lands on, then assert the reliable claim returns congested within a bounded
    // wall-clock rather than hanging.
    constexpr std::uint64_t       k_cells = 16;
    constexpr std::uint64_t       k_slot  = 64;
    ring_fixture<k_cells, k_slot> f;

    // Fill a full lap so the next claim position (k_cells) has a committed prior
    // occupant in its cell. A registered idle cursor keeps consumers_passed true so
    // the claim reaches the pin-clear spin rather than the cursor gate.
    std::uint32_t idx = 0;
    REQUIRE(f.ring.register_cursor(idx) == loan_status::ok);
    for(std::uint64_t i = 0; i < k_cells; ++i)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                         plexus::io::congestion::block, claim) == loan_status::ok);
        const std::uint32_t v = static_cast<std::uint32_t>(i);
        std::memcpy(claim.slab.data(), &v, sizeof(v));
        REQUIRE(f.ring.commit(claim.position, sizeof(v)) == loan_status::ok);
    }

    // Pin the cell position k_cells maps onto (cell 0): a live take() on the prior
    // occupant. announce_and_check_pin will never clear while this pin is held, so
    // the reliable claim must spin out its budget and surface congested.
    f.ring.refcount_at(0).fetch_add(1, std::memory_order_seq_cst);

    const auto                   before = plexus::testing::alloc_count();
    const auto                   start  = std::chrono::steady_clock::now();
    broadcast_ring::claim_result claim;
    const loan_status            st =
            f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                     plexus::io::congestion::block, claim);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const auto after   = plexus::testing::alloc_count();

    REQUIRE(st == loan_status::congested);
    REQUIRE(elapsed < std::chrono::seconds(1)); // bounded: it does NOT hang on the pin
    REQUIRE(after - before == 0);               // the bounded spin allocates nothing

    f.ring.refcount_at(0).fetch_sub(1, std::memory_order_seq_cst);
    f.ring.unregister_cursor(idx);
}
