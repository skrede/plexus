#include "test_priority_band_queue_common.h"

using namespace priority_band_fixture;

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
    q.pop_highest(); // a no-op on empty, never crashes
    REQUIRE_FALSE(q.has_work());
}

TEST_CASE("priority_band queue: FIFO order is preserved within a band", "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::normal);
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

TEST_CASE("priority_band queue: strict cross-band priority, a lower band index drains fully first", "[priority_band][forwarder]")
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

TEST_CASE("priority_band queue: block at a full band refuses the new frame and bumps blocked_count", "[priority_band][forwarder]")
{
    priority_band_queue q;
    const std::size_t b = band_of(priority::high);
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
    const std::size_t b = band_of(priority::high);
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
    const std::size_t b = band_of(priority::high);
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
