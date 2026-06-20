#include "test_priority_band_queue_common.h"

#include "support/alloc_counter.h"

using namespace priority_band_fixture;

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
