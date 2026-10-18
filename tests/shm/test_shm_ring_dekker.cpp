#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

// The overwrite-vs-pin Dekker race between a best_effort producer and a consumer pinning each slot via
// pin_if_current: a torn read (an overwrite stomping a live pin) would leave a pinned payload's halves disagreeing.
// The producer runs for at least k_min_publishes and until the consumer has verified k_target_pins pins, so coverage
// does not depend on how the scheduler interleaves the two threads; the tsan tree runs it to catch the race.

using namespace plexus::shm;

namespace {

constexpr std::uint32_t k_min_publishes = 20000;
constexpr std::uint64_t k_target_pins   = 1000;
constexpr auto k_deadline               = std::chrono::seconds(10);

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

struct race_state
{
    std::atomic<bool> torn{false};
    std::atomic<bool> producer_done{false};
    std::atomic<std::uint32_t> published{0};
    std::atomic<std::uint64_t> pins_verified{0};
};

void publish(broadcast_ring &ring, std::uint32_t value)
{
    broadcast_ring::claim_result claim;
    loan_status st;
    do
        st = ring.claim_with_policy(sizeof(payload), plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest, claim);
    while(st == loan_status::congested);
    if(st != loan_status::ok)
        return;
    const payload p{value, value};
    std::memcpy(claim.slab.data(), &p, sizeof(p));
    ring.commit(claim.position, sizeof(p));
}

bool coverage_met(std::uint32_t published, const race_state &state)
{
    return published >= k_min_publishes && state.pins_verified.load(std::memory_order_relaxed) >= k_target_pins;
}

void produce(broadcast_ring &ring, race_state &state)
{
    const auto deadline = std::chrono::steady_clock::now() + k_deadline;
    std::uint32_t value = 0;
    while(!coverage_met(value, state) && std::chrono::steady_clock::now() < deadline)
        publish(ring, ++value);
    state.published.store(value, std::memory_order_relaxed);
    state.producer_done.store(true, std::memory_order_release);
}

void verify_pinned(broadcast_ring &ring, std::uint64_t cursor, const broadcast_ring::consume_result &consumed, race_state &state)
{
    if(!ring.pin_if_current(cursor))
        return;
    payload p{};
    std::memcpy(&p, consumed.slab.data(), sizeof(p));
    if(p.a != p.b)
        state.torn.store(true, std::memory_order_relaxed);
    state.pins_verified.fetch_add(1, std::memory_order_relaxed);
    ring.refcount_at(cursor).fetch_sub(1, std::memory_order_seq_cst);
}

// A congested consume is a lapped cursor or a skip tombstone; both step forward like a consumed slot.
void consume(broadcast_ring &ring, std::uint32_t cursor_index, std::uint64_t cursor, race_state &state)
{
    while(!state.producer_done.load(std::memory_order_acquire))
    {
        broadcast_ring::consume_result consumed;
        const loan_status st = ring.consume(cursor, consumed);
        if(st == loan_status::empty)
            continue;
        if(st != loan_status::congested)
            verify_pinned(ring, cursor, consumed, state);
        ++cursor;
        ring.publish_cursor(cursor_index, cursor);
    }
}

}

TEST_CASE("ring_dekker: best_effort overwrite never tears a live pin", "[shm][ring_dekker]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot  = sizeof(payload);

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring) == loan_status::ok);

    std::uint32_t cursor_index = 0;
    REQUIRE(ring.register_cursor(cursor_index) == loan_status::ok);
    const std::uint64_t cursor = ring.tail_position();

    race_state state;
    std::thread consumer([&] { consume(ring, cursor_index, cursor, state); });
    std::thread producer([&] { produce(ring, state); });
    producer.join();
    consumer.join();
    ring.unregister_cursor(cursor_index);

    REQUIRE_FALSE(state.torn.load());
    REQUIRE(state.published.load() >= k_min_publishes);
    REQUIRE(state.pins_verified.load() >= k_target_pins);
}
