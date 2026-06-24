#include "test_fragmentation_common.h"

using namespace fragmentation_fixture;

namespace {

using test_reassembler = datagram::detail::reassembler<plexus::inproc::inproc_executor<testing::test_clock> &, plexus::inproc::inproc_timer<testing::test_clock>>;

std::vector<std::byte> seq_bytes(std::size_t n, int base)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((base + static_cast<int>(i)) & 0xFF);
    return v;
}

}

TEST_CASE("reassembler reassembles in-order fragments into one delivered message", "[fragment][reassemble]")
{
    testing::harness h;
    test_reassembler r{h.ex};

    std::optional<std::vector<std::byte>> delivered;
    r.on_deliver([&delivered](std::span<const std::byte> b) { delivered = std::vector<std::byte>(b.begin(), b.end()); });

    const auto a = seq_bytes(4, 0), b = seq_bytes(4, 100), c = seq_bytes(2, 200);
    CHECK(r.feed(1, 0, 3, a) == test_reassembler::outcome::admitted);
    CHECK(r.feed(1, 1, 3, b) == test_reassembler::outcome::admitted);
    CHECK(r.feed(1, 2, 3, c) == test_reassembler::outcome::completed);

    REQUIRE(delivered.has_value());
    REQUIRE(delivered->size() == 10);
    CHECK(std::equal(a.begin(), a.end(), delivered->begin()));
    CHECK(std::equal(c.begin(), c.end(), delivered->begin() + 8));
    CHECK(r.in_flight() == 0);
    CHECK(r.held_bytes() == 0);
}

TEST_CASE("reassembler reassembles out-of-order fragments into exactly one message", "[fragment][reassemble]")
{
    testing::harness h;
    test_reassembler r{h.ex};

    int                    deliveries = 0;
    std::vector<std::byte> got;
    r.on_deliver(
            [&](std::span<const std::byte> b)
            {
                ++deliveries;
                got.assign(b.begin(), b.end());
            });

    const auto a = seq_bytes(3, 1), b = seq_bytes(3, 50), c = seq_bytes(3, 90);
    CHECK(r.feed(7, 2, 3, c) == test_reassembler::outcome::admitted);
    CHECK(r.feed(7, 0, 3, a) == test_reassembler::outcome::admitted);
    CHECK(r.feed(7, 1, 3, b) == test_reassembler::outcome::completed);

    CHECK(deliveries == 1);
    REQUIRE(got.size() == 9);
    CHECK(std::equal(a.begin(), a.end(), got.begin())); // reassembled in index order
    CHECK(std::equal(c.begin(), c.end(), got.begin() + 6));
}

TEST_CASE("reassembler reclaims a stalled partial on the per-message timeout", "[fragment][reassemble]")
{
    testing::harness h;
    test_reassembler r{h.ex, {.per_message_timeout = 1000ms}};

    bool delivered = false;
    r.on_deliver([&](std::span<const std::byte>) { delivered = true; });

    CHECK(r.feed(3, 0, 4, seq_bytes(8, 0)) == test_reassembler::outcome::admitted);
    REQUIRE(r.in_flight() == 1);
    REQUIRE(r.held_bytes() == 8 + test_reassembler::structural_cost(4));

    h.advance(std::chrono::duration_cast<testing::test_clock::duration>(1500ms));

    CHECK_FALSE(delivered); // best-effort drop-whole: the lost-fragment message is gone
    CHECK(r.in_flight() == 0);
    CHECK(r.held_bytes() == 0);
}

TEST_CASE("reassembler rejects a new partial that would breach the total-memory cap", "[fragment][reassemble]")
{
    testing::harness h;
    const auto       one_msg = 10 + test_reassembler::structural_cost(2);
    test_reassembler r{h.ex, {.total_memory_cap = one_msg + 5}};

    // First message takes a fragment in-flight, consuming part of the cap.
    CHECK(r.feed(1, 0, 2, seq_bytes(10, 0)) == test_reassembler::outcome::admitted);
    REQUIRE(r.held_bytes() == one_msg);

    // A NEW msg_id whose first fragment would breach the cap is rejected; the in-progress
    // entry is untouched (no OOM, no eviction of live work).
    CHECK(r.feed(2, 0, 2, seq_bytes(10, 0)) == test_reassembler::outcome::dropped_cap);
    CHECK(r.in_flight() == 1);
    CHECK(r.held_bytes() == one_msg);
}

TEST_CASE("reassembler fails closed on malformed fragments without indexing past the span", "[fragment][reassemble]")
{
    testing::harness h;
    test_reassembler r{h.ex};

    CHECK(r.feed(1, 5, 3, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed);      // idx >= cnt
    CHECK(r.feed(1, 0, 0, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed);      // cnt == 0
    CHECK(r.feed(1, 0, 0xFFFF, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed); // cnt over max
    CHECK(r.in_flight() == 0);

    // A duplicate fragment is ignored (the first wins), the message still completes once.
    int deliveries = 0;
    r.on_deliver([&](std::span<const std::byte>) { ++deliveries; });
    CHECK(r.feed(9, 0, 2, seq_bytes(3, 1)) == test_reassembler::outcome::admitted);
    CHECK(r.feed(9, 0, 2, seq_bytes(3, 9)) == test_reassembler::outcome::admitted); // duplicate idx 0
    CHECK(r.feed(9, 1, 2, seq_bytes(3, 2)) == test_reassembler::outcome::completed);
    CHECK(deliveries == 1);
}
