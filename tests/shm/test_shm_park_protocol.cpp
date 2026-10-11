#include "parker_model.h"
#include "park_enumerator.h"

#include "plexus/shm/ring_layout.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <atomic>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {

using plexus::testing::futex_model;
using plexus::testing::model_step;
using plexus::testing::enumeration;
using plexus::testing::parker_model;
using plexus::testing::parked_waiter;
using plexus::testing::parker_signals;
using plexus::testing::parker_sequence;
using plexus::testing::interleaving_count;
using sequence = plexus::testing::interleaving_enumerator<futex_model>::sequence;

void store_park(futex_model &m, std::size_t word, std::uint32_t state)
{
    m.park[word].store(state, std::memory_order_seq_cst);
}

void take_snapshot(futex_model &m, std::size_t thread)
{
    m.snapshot[thread] = m.generation.load(std::memory_order_seq_cst);
}

sequence gated_signals(std::size_t producer, std::size_t word, std::uint32_t count)
{
    sequence steps;
    for(std::uint32_t i = 0; i < count; ++i)
    {
        steps.push_back(model_step(producer, [](futex_model &m) { m.generation.fetch_add(1, std::memory_order_seq_cst); }));
        steps.push_back(model_step(producer,
                                   [word](futex_model &m)
                                   {
                                       if(m.park[word].exchange(plexus::shm::k_park_notified, std::memory_order_seq_cst) == plexus::shm::k_park_parked)
                                           m.wake_all();
                                   }));
    }
    return steps;
}

sequence snapshot_first_waiter(std::size_t thread, std::size_t word)
{
    return {
            model_step(thread, [thread](futex_model &m) { take_snapshot(m, thread); }),
            model_step(thread, [word](futex_model &m) { store_park(m, word, plexus::shm::k_park_parked); }),
            model_step(thread, [thread](futex_model &m) { m.block(thread); }),
    };
}

template<typename Model>
enumeration count_lost_wakes(std::vector<typename plexus::testing::interleaving_enumerator<Model>::sequence> threads)
{
    plexus::testing::interleaving_enumerator<Model> enumerator{std::move(threads)};
    return enumerator.run([](const Model &m) { return m.no_lost_wake(); });
}

}

// The waiter publishes PARKED before it snapshots: the first signal's wake lands before the block and its snapshot
// already covers that bump, so the second signal finds NOTIFIED and skips the wake.
TEST_CASE("park_protocol the enumerator finds the lost wake when a waiter parks before it snapshots", "[shm][park_protocol]")
{
    const sequence waiter{
            model_step(0, [](futex_model &m) { store_park(m, 0, plexus::shm::k_park_parked); }),
            model_step(0, [](futex_model &m) { take_snapshot(m, 0); }),
            model_step(0, [](futex_model &m) { m.block(0); }),
    };

    const enumeration result = count_lost_wakes<futex_model>({waiter, gated_signals(1, 0, 2)});

    REQUIRE(result.interleavings == 35);
    REQUIRE(result.violations > 0);
}

// The second waiter's unpark erases the first waiter's PARKED on the shared word before the gated signal reads it.
TEST_CASE("park_protocol the enumerator finds the lost wake when two waiters share one park word", "[shm][park_protocol]")
{
    std::vector<sequence> threads;
    for(std::size_t waiter = 0; waiter < 2; ++waiter)
    {
        threads.push_back(snapshot_first_waiter(waiter, 0));
        threads.back().push_back(model_step(waiter, [](futex_model &m) { store_park(m, 0, plexus::shm::k_park_empty); }));
    }
    threads.push_back(gated_signals(2, 0, 1));

    const enumeration result = count_lost_wakes<futex_model>(std::move(threads));

    REQUIRE(result.interleavings == 3150);
    REQUIRE(result.violations > 0);
}

// A signal whose bump precedes the snapshot swaps the later PARKED to NOTIFIED with nobody blocked yet; the waiter then
// blocks on a snapshot that covers that bump, and the next signal finds the stale NOTIFIED and skips the wake.
TEST_CASE("park_protocol the enumerator finds the lost wake when a stale notification outlives a snapshot-first park", "[shm][park_protocol]")
{
    const enumeration result = count_lost_wakes<futex_model>({snapshot_first_waiter(0, 0), gated_signals(1, 0, 2)});

    REQUIRE(result.interleavings == 35);
    REQUIRE(result.violations > 0);
}

TEST_CASE("park_protocol no interleaving loses a wake for one waiter and one to three signals", "[shm][park_protocol]")
{
    const std::uint32_t signals = GENERATE(1u, 2u, 3u);

    const enumeration result = count_lost_wakes<parker_model>({parked_waiter(0), parker_signals(signals)});

    REQUIRE(result.interleavings == interleaving_count({5, 2 * signals}));
    REQUIRE(result.violations == 0);
}

TEST_CASE("park_protocol no interleaving loses a wake for two waiters and one to three signals", "[shm][park_protocol]")
{
    const std::uint32_t signals = GENERATE(1u, 2u, 3u);

    const enumeration result = count_lost_wakes<parker_model>({parked_waiter(0), parked_waiter(1), parker_signals(signals)});

    REQUIRE(result.interleavings == interleaving_count({5, 5, 2 * signals}));
    REQUIRE(result.violations == 0);
}

// Without the recheck a publish whose bump precedes the park store finds the word EMPTY and skips the wake, and the
// waiter then blocks on a snapshot that misses that publish.
TEST_CASE("park_protocol the enumerator finds the lost wake when a parker skips the generation recheck", "[shm][park_protocol]")
{
    parker_sequence waiter = parked_waiter(0);
    waiter[2]              = plexus::testing::parker_step(0, [](parker_model &m) { m.proceed[0] = true; });

    const enumeration result = count_lost_wakes<parker_model>({waiter, parker_signals(1)});

    REQUIRE(result.interleavings == interleaving_count({5, 2}));
    REQUIRE(result.violations > 0);
}
