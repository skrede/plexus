#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/slot_publisher.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/taken_message.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <vector>

// The adaptive spin-then-park consumer policy at the slot_subscriber take seam (Tuning 3).
// Proves the GENERIC consumer spin policy (the park MECHANISM stays the backend notifier):
//   * budget=0 (always-park): an empty ring returns empty immediately — the futex floor.
//   * budget>0: a message already in the ring is taken without spinning.
//   * budget>0: a message that LANDS during the spin window is CAUGHT (drained) without
//     falling through to empty — the back-to-back catch the spin exists for.
//   * budget>0, idle: take terminates and returns empty (the spin is bounded, never hangs),
//     so the notifier park takes over when genuinely idle.

using namespace plexus::shm;

namespace {

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
    std::span<std::byte> span() const noexcept
    {
        return {m_data, m_size};
    }

private:
    std::vector<std::byte> m_storage;
    std::byte *m_data{nullptr};
    std::size_t m_size{0};
};

struct fixture
{
    static constexpr std::uint64_t k_cells = 64;
    static constexpr std::uint64_t k_slot  = 64;

    backing_region control{control_region_bytes(k_cells)};
    backing_region slab{slab_region_bytes(k_cells, k_slot)};
    broadcast_ring ring;

    fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) == loan_status::ok);
    }

    // Publish one 4-byte value through a reliable+block publisher.
    void publish(std::uint32_t value)
    {
        slot_publisher pub{ring, plexus::io::reliability::reliable, plexus::io::congestion::block};
        loaned_buffer slot;
        REQUIRE(pub.loan(sizeof(value), slot) == loan_status::ok);
        std::memcpy(slot.bytes().data(), &value, sizeof(value));
        slot.set_filled(sizeof(value));
        REQUIRE(pub.publish(std::move(slot)) == loan_status::ok);
    }
};

}

TEST_CASE("shm.spin_park: budget 0 parks immediately on an empty ring", "[shm][spin_park]")
{
    fixture f;
    slot_subscriber sub{f.ring, /*spin_budget=*/0};

    taken_message msg;
    REQUIRE(sub.take(msg) == loan_status::empty); // no spin — the futex-park floor
}

TEST_CASE("shm.spin_park: a present message is taken regardless of the spin budget", "[shm][spin_park]")
{
    for(std::uint32_t budget : {0u, 64u, 256u, 4096u})
    {
        fixture f;
        slot_subscriber sub{f.ring, budget};
        f.publish(0xC0FFEE01u);

        taken_message msg;
        REQUIRE(sub.take(msg) == loan_status::ok);
        std::uint32_t got = 0;
        std::memcpy(&got, msg.as_wire_bytes().data(), sizeof(got));
        REQUIRE(got == 0xC0FFEE01u);
    }
}

TEST_CASE("shm.spin_park: a message landing during the spin window is caught without parking", "[shm][spin_park]")
{
    constexpr int k_iterations = 50;
    int caught_by_spin         = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f;
        // A large budget so the spin window comfortably covers the publisher's delay.
        slot_subscriber sub{f.ring, /*spin_budget=*/4'000'000};

        std::atomic<bool> go{false};
        std::thread producer(
                [&]
                {
                    while(!go.load(std::memory_order_acquire))
                    { /* spin to align */
                    }
                    std::this_thread::sleep_for(std::chrono::microseconds(20));
                    f.publish(0xABCD0000u | static_cast<std::uint32_t>(iter));
                });

        go.store(true, std::memory_order_release);
        taken_message msg;
        const loan_status st = sub.take(msg); // spins; should catch the late publish
        producer.join();

        if(st == loan_status::ok)
        {
            std::uint32_t got = 0;
            std::memcpy(&got, msg.as_wire_bytes().data(), sizeof(got));
            REQUIRE(got == (0xABCD0000u | static_cast<std::uint32_t>(iter)));
            ++caught_by_spin;
        }
    }
    // The spin must catch the in-window arrival the vast majority of the time (a rare
    // scheduler hiccup may exceed even the large budget, falling to empty — that is the
    // park path, not a defect). Require it caught at least most of the runs.
    REQUIRE(caught_by_spin >= k_iterations - 2);
}

TEST_CASE("shm.spin_park: an idle subscriber with a budget terminates to empty (parks)", "[shm][spin_park]")
{
    fixture f;
    slot_subscriber sub{f.ring, /*spin_budget=*/100'000};

    // No publisher: the spin must exhaust its budget and return empty (bounded, never
    // hangs) so the backend notifier park takes over.
    taken_message msg;
    REQUIRE(sub.take(msg) == loan_status::empty);
}
