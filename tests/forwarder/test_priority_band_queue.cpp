#include "support/alloc_counter.h"

#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/congestion.h"
#include "plexus/io/priority.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>

using plexus::wire_bytes;
using plexus::io::priority;
using plexus::io::congestion;
using plexus::io::detail::band_of;
using plexus::io::detail::k_egress_bands;
using plexus::io::detail::k_band_depth;
using plexus::io::detail::priority_band_queue;

namespace {

// Build a frame-owner the band slot holds (the production carrier): a span over an
// owning shared_ptr<vector>. Mirrors the forwarder's frame-once owner so the band
// tests drive the real slot representation rather than a borrowed span.
wire_bytes<> owned(const std::string &s)
{
    auto buf = std::make_shared<std::vector<std::byte>>(
            reinterpret_cast<const std::byte *>(s.data()),
            reinterpret_cast<const std::byte *>(s.data()) + s.size());
    std::span<const std::byte> view{*buf};
    return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
}

std::string body(const wire_bytes<> &w)
{
    return std::string{reinterpret_cast<const char *>(w.data()), w.size()};
}

}

TEST_CASE("priority_band queue: band_of maps realtime->0 high->1 normal->2 background->3",
          "[priority_band][forwarder]")
{
    REQUIRE(band_of(priority::realtime) == 0);
    REQUIRE(band_of(priority::high) == 1);
    REQUIRE(band_of(priority::normal) == 2);
    REQUIRE(band_of(priority::background) == 3);
    REQUIRE(band_of(priority::background) == k_egress_bands - 1);
}

TEST_CASE("priority_band queue: a fresh queue has no work and front_highest is null",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    REQUIRE_FALSE(q.has_work());
    REQUIRE(q.front_highest() == nullptr);
    q.pop_highest(); // a no-op on empty, never crashes
    REQUIRE_FALSE(q.has_work());
}

TEST_CASE("priority_band queue: FIFO order is preserved within a band",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t   b = band_of(priority::normal);
    REQUIRE(q.enqueue(b, congestion::block, owned("a")));
    REQUIRE(q.enqueue(b, congestion::block, owned("b")));
    REQUIRE(q.enqueue(b, congestion::block, owned("c")));

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
    REQUIRE(q.enqueue(band_of(priority::background), congestion::block, owned("bg0")));
    REQUIRE(q.enqueue(band_of(priority::background), congestion::block, owned("bg1")));
    REQUIRE(q.enqueue(band_of(priority::realtime), congestion::block, owned("rt0")));
    REQUIRE(q.enqueue(band_of(priority::realtime), congestion::block, owned("rt1")));

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
        REQUIRE(q.enqueue(b, congestion::block, owned("frame" + std::to_string(i))));
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
    const std::size_t   b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.blocked_count(b) == 0);
    // The band is full: block refuses, NO oldest evicted, the resident set unchanged.
    REQUIRE_FALSE(q.enqueue(b, congestion::block, owned("OVERFLOW")));
    REQUIRE(q.blocked_count(b) == 1);
    REQUIRE(body(*q.front_highest()) == "frame0");

    const auto drained = drain_bodies(q);
    REQUIRE(drained.size() == k_band_depth);
    REQUIRE(drained.front() == "frame0");
    REQUIRE(drained.back() == "frame" + std::to_string(k_band_depth - 1));
    for(const auto &f : drained)
        REQUIRE(f != "OVERFLOW");
}

TEST_CASE("priority_band queue: drop_newest at a full band refuses the new frame and bumps "
          "dropped_newest_count",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t   b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.dropped_newest_count(b) == 0);
    // The band is full: drop_newest refuses, NO oldest evicted, resident set unchanged.
    REQUIRE_FALSE(q.enqueue(b, congestion::drop_newest, owned("OVERFLOW")));
    REQUIRE(q.dropped_newest_count(b) == 1);
    REQUIRE(body(*q.front_highest()) == "frame0");

    const auto drained = drain_bodies(q);
    REQUIRE(drained.size() == k_band_depth);
    REQUIRE(drained.front() == "frame0");
    REQUIRE(drained.back() == "frame" + std::to_string(k_band_depth - 1));
    for(const auto &f : drained)
        REQUIRE(f != "OVERFLOW");
}

TEST_CASE("priority_band queue: drop_oldest at a full band evicts the oldest and admits the new "
          "frame",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t   b = band_of(priority::high);
    fill_band(q, b);

    REQUIRE(q.dropped_oldest_count(b) == 0);
    // The band is full: drop_oldest evicts frame0, admits OVERFLOW, count stays full,
    // and the new front is frame1 (frame0 is now ABSENT).
    REQUIRE(q.enqueue(b, congestion::drop_oldest, owned("OVERFLOW")));
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
    const std::size_t   b = band_of(priority::normal);
    // ONE pre-built owner is shared into the band on every admit (the frame-once carrier):
    // the band moves/holds the handle and addref-shares it, never allocating its own buffer.
    const wire_bytes<> frame = owned(std::string(64, 'x'));

    // Warm: fill the band to k_band_depth; leave it FULL so each subsequent enqueue takes
    // the drop_oldest evict path.
    for(std::size_t i = 0; i < k_band_depth; ++i)
        REQUIRE(q.enqueue(b, congestion::block, frame));

    // Each drop_oldest enqueue recycles the oldest slot (release the evicted owner) and
    // moves the addref-shared owner into the freed tail — a pure refcount op, no heap
    // traffic, so the global allocation count must not move.
    const std::size_t before = plexus::testing::alloc_count();
    for(int iter = 0; iter < 1000; ++iter)
        REQUIRE(q.enqueue(b, congestion::drop_oldest, frame));
    const std::size_t after = plexus::testing::alloc_count();
    REQUIRE(after == before);
}

TEST_CASE("priority_band queue: a steady enqueue/pop loop allocates nothing after warm-up",
          "[priority_band][forwarder]")
{
    priority_band_queue q;
    const wire_bytes<>  frame = owned(std::string(64, 'x'));

    // Warm: touch every band's full slot ring once, then drain it back to empty.
    for(std::size_t band = 0; band < k_egress_bands; ++band)
    {
        for(std::size_t i = 0; i < k_band_depth; ++i)
            REQUIRE(q.enqueue(band, congestion::block, frame));
        while(q.front_highest())
            q.pop_highest();
    }

    // The steady loop: enqueue a shared owner into each band and drain it; the band only
    // moves the owner handle (addref/release), so the global allocation count must not move.
    const std::size_t before = plexus::testing::alloc_count();
    for(int iter = 0; iter < 1000; ++iter)
        for(std::size_t band = 0; band < k_egress_bands; ++band)
        {
            REQUIRE(q.enqueue(band, congestion::block, frame));
            const auto *node = q.front_highest();
            REQUIRE(node != nullptr);
            q.pop_highest();
        }
    const std::size_t after = plexus::testing::alloc_count();
    REQUIRE(after == before);
}
