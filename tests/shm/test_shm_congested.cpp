#include "support/alloc_counter.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <vector>

// Backpressure + observable congestion: a best_effort ring with every slot pinned
// returns congested off send() (OBSERVABLE, never a silent drop). A reliable
// producer with a lagging consumer blocks losslessly -- every value arrives in
// order, none skipped. And the reliable blocking spin/yield path allocates nothing
// across N sends (the alloc-counter idiom).

using namespace plexus::io::shm;

namespace {

// A no-op notifier satisfying the seam (this TU exercises the loan/publish gate,
// not the wakeup; the channel still requires a notifier reference).
struct null_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(notifier<null_notifier>, "null_notifier must satisfy the notifier seam");

struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + k_cache_line - 1) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data       = reinterpret_cast<std::byte *>(aligned);
        m_size       = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

template<std::uint64_t Cells, std::uint64_t Slot>
struct ring_fixture
{
    backing_region control{control_region_bytes(Cells)};
    backing_region slab{slab_region_bytes(Cells, Slot)};
    broadcast_ring ring;
    null_notifier  notify;

    ring_fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), Cells, Slot, ring) ==
                loan_status::ok);
    }
};

std::uint32_t as_u32(std::span<const std::byte> b)
{
    std::uint32_t v = 0;
    std::memcpy(&v, b.data(), sizeof(v));
    return v;
}

}

TEST_CASE("shm.congested a fully-pinned best_effort ring surfaces congested off send()",
          "[shm][congested]")
{
    ring_fixture<16, 64> f;
    // A best_effort + drop channel: it overwrites the latest and NEVER blocks; its
    // own gate returns congested only when a full lap is pinned by live takes.
    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::best_effort,
                                       plexus::io::congestion::drop_newest);

    // Pin every cell with a held take so no slot is recyclable. We pin directly on
    // the ring (the Dekker reader half) to set up the fully-congested state.
    std::uint32_t idx = 0;
    REQUIRE(f.ring.register_cursor(idx) == loan_status::ok);
    std::uint64_t cursor = f.ring.tail_position();
    for(std::uint64_t i = 0; i < 16; ++i)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(f.ring.claim_with_policy(
                        sizeof(std::uint32_t), plexus::io::reliability::best_effort,
                        plexus::io::congestion::drop_newest, claim) == loan_status::ok);
        const std::uint32_t v = 0xBEEF0000u | static_cast<std::uint32_t>(i);
        std::memcpy(claim.slab.data(), &v, sizeof(v));
        REQUIRE(f.ring.commit(claim.position, sizeof(v)) == loan_status::ok);
        REQUIRE(f.ring.pin_if_current(cursor) == true);
        ++cursor;
    }

    // Every slot is pinned: a best_effort send can find no recyclable cell and the
    // status is OBSERVABLE on the return -- the value is not silently dropped.
    std::uint32_t payload = 0xFEEDFACEu;
    std::byte     bytes[sizeof(payload)];
    std::memcpy(bytes, &payload, sizeof(payload));
    REQUIRE(channel.send(std::span<const std::byte>(bytes, sizeof(bytes))) ==
            loan_status::congested);

    // Release the lap so the fixture tears down clean.
    for(std::uint64_t i = 0; i < 16; ++i)
        f.ring.refcount_at(i).fetch_sub(1);
    f.ring.unregister_cursor(idx);
}

TEST_CASE("shm.backpressure a reliable producer blocks losslessly on a lagging consumer",
          "[shm][backpressure]")
{
    constexpr std::uint64_t   k_cells = 16;
    constexpr int             k_total = 4000; // many laps so the producer must block repeatedly
    ring_fixture<k_cells, 64> f;

    // The producer blocks (reliable + block) on the slowest registered cursor: it
    // cannot lap an unconsumed value, so it back-pressures until the consumer drains.
    // The channel's OWN subscriber is the sole registered cursor (the co-located
    // loopback) -- the reader thread drains it, advancing that cursor so the
    // back-pressured producer resumes.
    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable,
                                       plexus::io::congestion::block);

    std::atomic<bool> producer_done{false};

    // Consumer thread: drain the channel in order, recording a strict 0..k_total-1
    // sequence with NO value skipped (the lossless proof). It keeps up just enough to
    // unblock the back-pressured producer. The reader allocates only via the reserved
    // vector below (a test bookkeeping concern, not the channel path).
    std::vector<std::uint32_t> seen;
    seen.reserve(k_total);
    std::thread reader(
            [&]
            {
                shm_channel<null_notifier>::deliver_fn deliver =
                        [&](plexus::wire_bytes<shm_slot_owner> wb) { seen.push_back(as_u32(wb)); };
                while(seen.size() < static_cast<std::size_t>(k_total))
                {
                    channel.drain(deliver);
                    if(seen.size() < static_cast<std::size_t>(k_total))
                        std::this_thread::yield();
                }
            });

    // Producer thread (this thread): N blocking reliable sends. Each blocks on the
    // lagging reader and resumes once a slot frees -- proving the gate holds without
    // dropping.
    for(int i = 0; i < k_total; ++i)
    {
        const std::uint32_t v = static_cast<std::uint32_t>(i);
        std::byte           bytes[sizeof(v)];
        std::memcpy(bytes, &v, sizeof(v));
        REQUIRE(channel.send(std::span<const std::byte>(bytes, sizeof(bytes))) == loan_status::ok);
    }
    producer_done.store(true);
    reader.join();

    // Lossless + in-order: every value 0..k_total-1 arrived exactly once, in order.
    REQUIRE(seen.size() == static_cast<std::size_t>(k_total));
    for(int i = 0; i < k_total; ++i)
        REQUIRE(seen[static_cast<std::size_t>(i)] == static_cast<std::uint32_t>(i));
}

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
