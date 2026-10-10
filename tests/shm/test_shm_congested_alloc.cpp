#include "test_shm_congested_common.h"

#include "support/alloc_counter.h"

using namespace shm_congested_fixture;

TEST_CASE("shm.backpressure the reliable blocking spin allocates nothing", "[shm][backpressure]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_total = 2000;
    ring_fixture<k_cells, 64> f;

    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable, plexus::io::congestion::block);

    // The deliver callback counts drained messages into an atomic (NO heap in its
    // body). It is constructed BEFORE the snapshot, so the one-time
    // move_only_function construction never falls inside the measured window.
    std::atomic<std::uint64_t> drained{0};
    shm_channel<null_notifier>::deliver_fn deliver = [&](plexus::wire_bytes<shm_slot_owner>) { drained.fetch_add(1, std::memory_order_release); };

    std::atomic<std::uint64_t> expected{k_total};
    // Draining only a full lap (or the final remainder) makes the producer's next claim meet a
    // full ring, so every run exercises the reliable spin instead of leaving it to scheduling.
    const auto lap_outstanding = [&]
    {
        const std::uint64_t tail = f.ring.tail_position();
        return tail - drained.load(std::memory_order_acquire) >= k_cells || tail >= expected.load(std::memory_order_acquire);
    };

    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};
    std::thread reader(
            [&]
            {
                started.store(true, std::memory_order_release);
                while(drained.load(std::memory_order_acquire) < expected.load(std::memory_order_acquire))
                {
                    if(lap_outstanding())
                        channel.drain(deliver);
                    else
                        std::this_thread::yield();
                }
                finished.store(true, std::memory_order_release);
            });

    // Thread start-up allocates under a sanitizer runtime (pthread_getattr_np reads the stack
    // bounds), so the window spans only the running reader: opened after it starts, closed before the join.
    while(!started.load(std::memory_order_acquire))
        std::this_thread::yield();

    const std::size_t before = plexus::testing::alloc_count();

    loan_status status = loan_status::ok;
    std::uint64_t sent = 0;
    while(sent < k_total && status == loan_status::ok)
    {
        const std::uint32_t v = static_cast<std::uint32_t>(sent);
        std::byte bytes[sizeof(v)];
        std::memcpy(bytes, &v, sizeof(v));
        status = channel.send(std::span<const std::byte>(bytes, sizeof(bytes)));
        if(status == loan_status::ok)
            ++sent;
    }
    // A rejected send never arrives; lowering the target lets the reader finish rather than hang.
    expected.store(sent, std::memory_order_release);
    while(!finished.load(std::memory_order_acquire))
        std::this_thread::yield();
    const std::size_t after = plexus::testing::alloc_count();
    reader.join();

    REQUIRE(status == loan_status::ok);
    REQUIRE(drained.load(std::memory_order_acquire) == k_total);
    // The steady-state send loop (incl. the reliable spin/yield back-pressure path)
    // and the drain loop allocated NOTHING: no kernel object, no heap -- the
    // determinism the safety/drone use requires.
    REQUIRE(after - before == 0);
}

TEST_CASE("shm.congested a reliable claim on a pinned cell returns congested past the spin budget", "[shm][congested][spin_budget]")
{
    // A reliable claim whose target cell is still held by a live take() pin used to
    // spin the producer core indefinitely. It now relaxes the core a bounded number
    // of iterations and returns congested (the slow consumer holds a legit loan --
    // surface it, never pin 100% core). We pin the cell the next contiguous claim
    // lands on, then assert the reliable claim returns congested within a bounded
    // wall-clock rather than hanging.
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot  = 64;
    ring_fixture<k_cells, k_slot> f;

    // Fill a full lap so the next claim position (k_cells) has a committed prior
    // occupant in its cell. A registered idle cursor keeps consumers_passed true so
    // the claim reaches the pin-clear spin rather than the cursor gate.
    std::uint32_t idx = 0;
    REQUIRE(f.ring.register_cursor(idx) == loan_status::ok);
    for(std::uint64_t i = 0; i < k_cells; ++i)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) == loan_status::ok);
        const std::uint32_t v = static_cast<std::uint32_t>(i);
        std::memcpy(claim.slab.data(), &v, sizeof(v));
        REQUIRE(f.ring.commit(claim.position, sizeof(v)) == loan_status::ok);
    }

    // Pin the cell position k_cells maps onto (cell 0): a live take() on the prior
    // occupant. announce_and_check_pin will never clear while this pin is held, so
    // the reliable claim must spin out its budget and surface congested.
    f.ring.refcount_at(0).fetch_add(1, std::memory_order_seq_cst);

    const auto before = plexus::testing::alloc_count();
    const auto start  = std::chrono::steady_clock::now();
    broadcast_ring::claim_result claim;
    const loan_status st = f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable, plexus::io::congestion::block, claim);
    const auto elapsed   = std::chrono::steady_clock::now() - start;
    const auto after     = plexus::testing::alloc_count();

    REQUIRE(st == loan_status::congested);
    REQUIRE(elapsed < std::chrono::seconds(1)); // bounded: it does NOT hang on the pin
    REQUIRE(after - before == 0);               // the bounded spin allocates nothing

    f.ring.refcount_at(0).fetch_sub(1, std::memory_order_seq_cst);
    f.ring.unregister_cursor(idx);
}
