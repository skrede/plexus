#include "test_shm_congested_common.h"

using namespace shm_congested_fixture;

TEST_CASE("shm.congested a fully-pinned best_effort ring surfaces congested off send()", "[shm][congested]")
{
    ring_fixture<16, 64> f;
    // A best_effort + drop channel: it overwrites the latest and NEVER blocks; its
    // own gate returns congested only when a full lap is pinned by live takes.
    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest);

    // Pin every cell with a held take so no slot is recyclable. We pin directly on
    // the ring (the Dekker reader half) to set up the fully-congested state.
    std::uint32_t idx = 0;
    REQUIRE(f.ring.register_cursor(idx) == loan_status::ok);
    std::uint64_t cursor = f.ring.tail_position();
    for(std::uint64_t i = 0; i < 16; ++i)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest, claim) == loan_status::ok);
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
    REQUIRE(channel.send(std::span<const std::byte>(bytes, sizeof(bytes))) == loan_status::congested);

    // Release the lap so the fixture tears down clean.
    for(std::uint64_t i = 0; i < 16; ++i)
        f.ring.refcount_at(i).fetch_sub(1);
    f.ring.unregister_cursor(idx);
}

TEST_CASE("shm.backpressure a reliable producer blocks losslessly on a lagging consumer", "[shm][backpressure]")
{
    constexpr std::uint64_t   k_cells = 16;
    constexpr int             k_total = 4000; // many laps so the producer must block repeatedly
    ring_fixture<k_cells, 64> f;

    // The producer blocks (reliable + block) on the slowest registered cursor: it
    // cannot lap an unconsumed value, so it back-pressures until the consumer drains.
    // The channel's OWN subscriber is the sole registered cursor (the co-located
    // loopback) -- the reader thread drains it, advancing that cursor so the
    // back-pressured producer resumes.
    shm_channel<null_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable, plexus::io::congestion::block);

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
                shm_channel<null_notifier>::deliver_fn deliver = [&](plexus::wire_bytes<shm_slot_owner> wb) { seen.push_back(as_u32(wb)); };
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
