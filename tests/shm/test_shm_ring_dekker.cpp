#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

// The overwrite-vs-pin Dekker race, driven in a tight loop across a producer and
// a consumer thread: the consumer pins each slot it observes via pin_if_current
// (the Dekker reader half) and asserts byte-equality of every value it manages to
// pin -- a torn read (a best_effort overwrite stomping a live pin) would corrupt
// the payload. N>=10000 publishes per run; the tsan run is the phase-boundary
// gate (Wave 6) -- this is the looped-logic proof.

using namespace plexus::shm;

namespace {

struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + (k_cache_line - 1)) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
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

// Each published value carries a self-describing pattern (two copies of a 32-bit
// counter): a torn read would leave the two halves disagreeing.
struct payload
{
    std::uint32_t a;
    std::uint32_t b;
};

}

TEST_CASE("ring_dekker: best_effort overwrite never tears a live pin", "[shm][ring_dekker]")
{
    constexpr std::uint64_t k_cells     = 16;
    constexpr std::uint64_t k_slot      = sizeof(payload);
    constexpr std::uint64_t k_publishes = 20000;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) == loan_status::ok);

    std::atomic<bool> torn{false};
    std::atomic<std::uint64_t> pins_verified{0};

    std::thread producer(
            [&]
            {
                for(std::uint64_t i = 1; i <= k_publishes; ++i)
                {
                    broadcast_ring::claim_result claim;
                    loan_status st;
                    do
                        st = ring.claim_with_policy(sizeof(payload), plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest, claim);
                    while(st == loan_status::congested);
                    if(st != loan_status::ok)
                        continue;
                    const auto v = static_cast<std::uint32_t>(i);
                    payload p{v, v};
                    std::memcpy(claim.slab.data(), &p, sizeof(p));
                    ring.commit(claim.position, sizeof(p));
                }
            });

    std::thread consumer(
            [&]
            {
                std::uint32_t cursor_index = 0;
                if(ring.register_cursor(cursor_index) != loan_status::ok)
                    return;
                std::uint64_t cursor = ring.tail_position();
                for(std::uint64_t spins = 0; spins < k_publishes * 64; ++spins)
                {
                    broadcast_ring::consume_result consumed;
                    const loan_status st = ring.consume(cursor, consumed);
                    if(st == loan_status::empty)
                        continue;
                    if(st == loan_status::congested)
                    {
                        ++cursor; // lapped or skip tombstone: step forward
                        ring.publish_cursor(cursor_index, cursor);
                        continue;
                    }
                    // ok: try to pin the slot via the Dekker reader half, then verify the
                    // bytes are internally consistent while the pin is held.
                    if(ring.pin_if_current(cursor))
                    {
                        payload p{};
                        std::memcpy(&p, consumed.slab.data(), sizeof(p));
                        if(p.a != p.b)
                            torn.store(true, std::memory_order_relaxed);
                        pins_verified.fetch_add(1, std::memory_order_relaxed);
                        ring.refcount_at(cursor).fetch_sub(1, std::memory_order_seq_cst);
                    }
                    ++cursor;
                    ring.publish_cursor(cursor_index, cursor);
                }
                ring.unregister_cursor(cursor_index);
            });

    producer.join();
    consumer.join();

    REQUIRE_FALSE(torn.load());
    // The consumer must have successfully pinned and verified at least some
    // slots (the race is real, not vacuously empty).
    REQUIRE(pins_verified.load() > 0);
}
