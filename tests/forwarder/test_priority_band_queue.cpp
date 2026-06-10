#include "support/alloc_counter.h"

#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/congestion.h"
#include "plexus/io/priority.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>

using plexus::io::priority;
using plexus::io::congestion;
using plexus::io::detail::band_of;
using plexus::io::detail::k_egress_bands;
using plexus::io::detail::k_band_depth;
using plexus::io::detail::priority_band_queue;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string body(const std::vector<std::byte> &v)
{
    return std::string{reinterpret_cast<const char *>(v.data()), v.size()};
}

}

TEST_CASE("priority_band queue: band_of maps realtime->0 high->1 normal->2 background->3", "[priority_band][forwarder]")
{
    REQUIRE(band_of(priority::realtime) == 0);
    REQUIRE(band_of(priority::high) == 1);
    REQUIRE(band_of(priority::normal) == 2);
    REQUIRE(band_of(priority::background) == 3);
    REQUIRE(band_of(priority::background) == k_egress_bands - 1);
}

TEST_CASE("priority_band queue: a fresh queue has no work and front_highest is null", "[priority_band][forwarder]")
{
    priority_band_queue q;
    REQUIRE_FALSE(q.has_work());
    REQUIRE(q.front_highest() == nullptr);
    q.pop_highest();   // a no-op on empty, never crashes
    REQUIRE_FALSE(q.has_work());
}

TEST_CASE("priority_band queue: FIFO order is preserved within a band", "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::normal);
    REQUIRE(q.enqueue(b, congestion::block, as_bytes("a")));
    REQUIRE(q.enqueue(b, congestion::block, as_bytes("b")));
    REQUIRE(q.enqueue(b, congestion::block, as_bytes("c")));

    REQUIRE(body(*q.front_highest()) == "a");
    q.pop_highest();
    REQUIRE(body(*q.front_highest()) == "b");
    q.pop_highest();
    REQUIRE(body(*q.front_highest()) == "c");
    q.pop_highest();
    REQUIRE_FALSE(q.has_work());
}

TEST_CASE("priority_band queue: strict cross-band priority, a lower band index drains fully first",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    // background (band 3) gets two frames, then realtime (band 0) gets two — the
    // realtime band must drain completely before any background frame surfaces.
    REQUIRE(q.enqueue(band_of(priority::background), congestion::block, as_bytes("bg0")));
    REQUIRE(q.enqueue(band_of(priority::background), congestion::block, as_bytes("bg1")));
    REQUIRE(q.enqueue(band_of(priority::realtime), congestion::block, as_bytes("rt0")));
    REQUIRE(q.enqueue(band_of(priority::realtime), congestion::block, as_bytes("rt1")));

    std::vector<std::string> order;
    while(const auto *node = q.front_highest())
    {
        order.emplace_back(body(*node));
        q.pop_highest();
    }
    REQUIRE(order == std::vector<std::string>{"rt0", "rt1", "bg0", "bg1"});
}

namespace {

// Fill a band to k_band_depth with bodies "frame0".."frame{N-1}".
void fill_band(priority_band_queue &q, std::size_t b)
{
    for(std::size_t i = 0; i < k_band_depth; ++i)
        REQUIRE(q.enqueue(b, congestion::block, as_bytes("frame" + std::to_string(i))));
}

// Drain a queue front-to-back, capturing each body in send order.
std::vector<std::string> drain_bodies(priority_band_queue &q)
{
    std::vector<std::string> out;
    while(const auto *node = q.front_highest())
    {
        out.emplace_back(body(*node));
        q.pop_highest();
    }
    return out;
}

}

TEST_CASE("priority_band queue: block at a full band refuses the new frame and bumps blocked_count",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.blocked_count(b) == 0);
    // The band is full: block refuses, NO oldest evicted, the resident set unchanged.
    REQUIRE_FALSE(q.enqueue(b, congestion::block, as_bytes("OVERFLOW")));
    REQUIRE(q.blocked_count(b) == 1);
    REQUIRE(body(*q.front_highest()) == "frame0");

    const auto drained = drain_bodies(q);
    REQUIRE(drained.size() == k_band_depth);
    REQUIRE(drained.front() == "frame0");
    REQUIRE(drained.back() == "frame" + std::to_string(k_band_depth - 1));
    for(const auto &f : drained)
        REQUIRE(f != "OVERFLOW");
}

TEST_CASE("priority_band queue: drop_newest at a full band refuses the new frame and bumps dropped_newest_count",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.dropped_newest_count(b) == 0);
    // The band is full: drop_newest refuses, NO oldest evicted, resident set unchanged.
    REQUIRE_FALSE(q.enqueue(b, congestion::drop_newest, as_bytes("OVERFLOW")));
    REQUIRE(q.dropped_newest_count(b) == 1);
    REQUIRE(body(*q.front_highest()) == "frame0");

    const auto drained = drain_bodies(q);
    REQUIRE(drained.size() == k_band_depth);
    REQUIRE(drained.front() == "frame0");
    REQUIRE(drained.back() == "frame" + std::to_string(k_band_depth - 1));
    for(const auto &f : drained)
        REQUIRE(f != "OVERFLOW");
}

TEST_CASE("priority_band queue: drop_oldest at a full band evicts the oldest and admits the new frame",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.dropped_oldest_count(b) == 0);
    // The band is full: drop_oldest evicts frame0, admits OVERFLOW, count stays full,
    // and the new front is frame1 (frame0 is now ABSENT).
    REQUIRE(q.enqueue(b, congestion::drop_oldest, as_bytes("OVERFLOW")));
    REQUIRE(q.dropped_oldest_count(b) == 1);
    REQUIRE(body(*q.front_highest()) == "frame1");

    // The surviving window is EXACTLY {frame1..frame{N-1}, OVERFLOW} in FIFO order.
    std::vector<std::string> expected;
    for(std::size_t i = 1; i < k_band_depth; ++i)
        expected.emplace_back("frame" + std::to_string(i));
    expected.emplace_back("OVERFLOW");

    const auto drained = drain_bodies(q);
    REQUIRE(drained == expected);
    for(const auto &f : drained)
        REQUIRE(f != "frame0");
}

TEST_CASE("priority_band queue: a warm drop_oldest evict-and-admit loop allocates nothing",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::normal);
    const std::string payload(64, 'x');

    // Warm: fill the band to k_band_depth once so every pooled slot grows its capacity;
    // leave the band FULL so each subsequent enqueue takes the drop_oldest evict path.
    for(std::size_t i = 0; i < k_band_depth; ++i)
        REQUIRE(q.enqueue(b, congestion::block, as_bytes(payload)));

    // Each drop_oldest enqueue recycles the oldest slot (pop) and assigns into the freed
    // tail — no slot is allocated or freed, so the global allocation count must not move.
    const std::size_t before = plexus::testing::alloc_count();
    for(int iter = 0; iter < 1000; ++iter)
        REQUIRE(q.enqueue(b, congestion::drop_oldest, as_bytes(payload)));
    const std::size_t after = plexus::testing::alloc_count();
    REQUIRE(after == before);
}

TEST_CASE("priority_band queue: a steady enqueue/pop loop allocates nothing after warm-up",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::string payload(64, 'x');

    // Warm: touch every band's full slot ring once so each pooled slot grows its
    // capacity, then drain it back to empty.
    for(std::size_t band = 0; band < k_egress_bands; ++band)
    {
        for(std::size_t i = 0; i < k_band_depth; ++i)
            REQUIRE(q.enqueue(band, congestion::block, as_bytes(payload)));
        while(q.front_highest())
            q.pop_highest();
    }

    // The steady loop: enqueue a frame into each band and drain it; the grown slots
    // are reused via assign, so the global allocation count must not move.
    const std::size_t before = plexus::testing::alloc_count();
    for(int iter = 0; iter < 1000; ++iter)
        for(std::size_t band = 0; band < k_egress_bands; ++band)
        {
            REQUIRE(q.enqueue(band, congestion::block, as_bytes(payload)));
            const auto *node = q.front_highest();
            REQUIRE(node != nullptr);
            q.pop_highest();
        }
    const std::size_t after = plexus::testing::alloc_count();
    REQUIRE(after == before);
}
