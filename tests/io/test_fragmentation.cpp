#include "plexus/io/detail/reassembler.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/mtu_budget.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/testing/harness.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

using namespace plexus;
using namespace std::chrono_literals;

namespace {

std::vector<std::byte> bytes_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> out;
    out.reserve(vals.size());
    for(int v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

TEST_CASE("wire fragment header round-trips msg_id/frag_idx/frag_cnt and bytes", "[fragment][wire]")
{
    const auto payload = bytes_of({0xDE, 0xAD, 0xBE, 0xEF, 0x10});
    std::vector<std::byte> out;
    wire::wrap_udp_fragment_into(out, wire::udp_envelope_kind::reliable_arq, /*seq*/ 7,
                                 /*msg_id*/ 0x1234, /*frag_idx*/ 2, /*frag_cnt*/ 5, payload);

    REQUIRE(out.size() == wire::udp_fragment_header_overhead + payload.size());

    auto env = wire::unwrap_udp(out);
    REQUIRE(env.has_value());
    REQUIRE(env->fragmented);
    REQUIRE(env->kind == wire::udp_envelope_kind::reliable_arq);
    REQUIRE(env->seq == 7);

    auto frag = wire::decode_udp_fragment_header(env->frame);
    REQUIRE(frag.has_value());
    CHECK(frag->msg_id == 0x1234);
    CHECK(frag->frag_idx == 2);
    CHECK(frag->frag_cnt == 5);
    REQUIRE(frag->payload.size() == payload.size());
    CHECK(std::equal(frag->payload.begin(), frag->payload.end(), payload.begin()));
}

TEST_CASE("wire fragment header carries a zero-length fragment payload", "[fragment][wire]")
{
    std::vector<std::byte> out;
    wire::wrap_udp_fragment_into(out, wire::udp_envelope_kind::best_effort, 0, 9, 0, 1, {});
    auto env = wire::unwrap_udp(out);
    REQUIRE(env.has_value());
    auto frag = wire::decode_udp_fragment_header(env->frame);
    REQUIRE(frag.has_value());
    CHECK(frag->frag_cnt == 1);
    CHECK(frag->payload.empty());
}

TEST_CASE("decode_udp_fragment_header fails closed one byte below the sub-header", "[fragment][wire]")
{
    std::array<std::byte, wire::udp_fragment_subheader - 1> truncated{};
    auto frag = wire::decode_udp_fragment_header(std::span<const std::byte>{truncated});
    CHECK_FALSE(frag.has_value());

    std::array<std::byte, 0> empty{};
    CHECK_FALSE(wire::decode_udp_fragment_header(std::span<const std::byte>{empty}).has_value());
}

TEST_CASE("the unfragmented wrap path keeps the 3-byte envelope unchanged", "[fragment][wire]")
{
    const auto frame = bytes_of({1, 2, 3, 4});
    std::vector<std::byte> out;
    wire::wrap_udp_into(out, wire::udp_envelope_kind::best_effort, 42, frame);

    REQUIRE(out.size() == wire::udp_envelope_overhead + frame.size());
    auto env = wire::unwrap_udp(out);
    REQUIRE(env.has_value());
    CHECK_FALSE(env->fragmented);
}

namespace {

struct recorded_fragment
{
    std::uint32_t idx;
    std::uint32_t cnt;
    std::vector<std::byte> bytes;
};

std::vector<recorded_fragment> collect(std::span<const std::byte> payload, std::size_t budget,
                                       bool aead_decorated = false)
{
    std::vector<recorded_fragment> seen;
    io::fragment_sink sink = [&seen](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> b) {
        seen.push_back({idx, cnt, std::vector<std::byte>(b.begin(), b.end())});
    };
    const auto returned = io::split(payload, budget, /*msg_id*/ 1, sink, aead_decorated);
    REQUIRE(returned == seen.size());
    return seen;
}

}

TEST_CASE("the splitter fragments against the default budget and honors an upward override", "[fragment][split][mtu]")
{
    // The splitter consults the mtu_budget the channel passes: at the conservative 1200-byte
    // default a payload above it fragments, and an explicit higher budget (the tunable knob)
    // yields fewer, larger fragments — the per-datagram budget is configurable up, never
    // silently clamped to the default.
    const std::vector<std::byte> payload(io::mtu_budget{}.max_payload * 4);

    const auto at_default = collect(payload, io::mtu_budget{}.max_payload);
    const auto at_override = collect(payload, io::mtu_budget{.max_payload = 8192}.max_payload);

    REQUIRE(at_default.size() > 1);                          // genuinely fragmented at 1200
    CHECK(at_override.size() < at_default.size());           // the larger budget fragments less
    CHECK(at_override.front().bytes.size() > at_default.front().bytes.size());
}

TEST_CASE("split against a budget yields one fragment when the payload fits", "[fragment][split]")
{
    const std::vector<std::byte> payload(100);
    auto frags = collect(payload, /*budget*/ 1400);
    REQUIRE(frags.size() == 1);
    CHECK(frags[0].idx == 0);
    CHECK(frags[0].cnt == 1);
    CHECK(frags[0].bytes.size() == 100);
}

TEST_CASE("split a payload just over the effective budget yields two fragments", "[fragment][split]")
{
    const std::size_t budget = 100;
    const std::size_t eff = io::effective_fragment_budget(budget);
    const std::vector<std::byte> payload(eff + 1);

    auto frags = collect(payload, budget);
    REQUIRE(frags.size() == 2);
    CHECK(frags[0].cnt == 2);
    CHECK(frags[0].bytes.size() == eff);
    CHECK(frags[1].idx == 1);
    CHECK(frags[1].bytes.size() == 1);
}

TEST_CASE("split a large payload yields N fragments, N-1 full-sized in index order", "[fragment][split]")
{
    const std::size_t budget = 200;
    const std::size_t eff = io::effective_fragment_budget(budget);
    const std::vector<std::byte> payload(eff * 4 + 7);

    auto frags = collect(payload, budget);
    REQUIRE(frags.size() == 5);

    std::size_t reassembled = 0;
    for(std::size_t i = 0; i < frags.size(); ++i)
    {
        CHECK(frags[i].idx == i);          // ascending index order
        CHECK(frags[i].cnt == 5);
        if(i + 1 < frags.size())
            CHECK(frags[i].bytes.size() == eff);   // N-1 full-sized
        reassembled += frags[i].bytes.size();
    }
    CHECK(frags.back().bytes.size() == 7);          // the remainder
    CHECK(reassembled == payload.size());
}

TEST_CASE("an AEAD-decorated split leaves room for the per-fragment seal overhead so a sealed fragment fits the MTU",
          "[fragment][split][aead]")
{
    const std::size_t budget = 1400;
    // A payload large enough to force several full-budget fragments on each path.
    const std::vector<std::byte> payload(io::effective_fragment_budget(budget) * 3 + 13);

    const auto aead = collect(payload, budget, /*aead_decorated=*/true);
    REQUIRE(aead.size() >= 2);
    // Every emitted fragment, once the datagram decorator prepends seq+epoch and appends
    // the tag, stays inside the transport budget — the previously-overrunning case fits.
    for(const auto &f : aead)
        CHECK(f.bytes.size() + io::k_aead_fragment_overhead <= budget);

    // The AEAD budget is strictly tighter than the plaintext budget, so a sealed fragment
    // sized to the plaintext budget would have overrun the MTU by exactly the seal overhead.
    CHECK(io::effective_fragment_budget(budget, /*aead_decorated=*/true) + io::k_aead_fragment_overhead
          == io::effective_fragment_budget(budget, /*aead_decorated=*/false));
}

TEST_CASE("a non-AEAD split is byte-identical to the default path", "[fragment][split][aead]")
{
    const std::size_t budget = 512;
    const std::vector<std::byte> payload(io::effective_fragment_budget(budget) * 2 + 5);

    const auto plain = collect(payload, budget, /*aead_decorated=*/false);
    const auto deflt = collect(payload, budget);
    REQUIRE(plain.size() == deflt.size());
    for(std::size_t i = 0; i < plain.size(); ++i)
    {
        CHECK(plain[i].idx == deflt[i].idx);
        CHECK(plain[i].cnt == deflt[i].cnt);
        CHECK(plain[i].bytes == deflt[i].bytes);
    }
}

namespace {

using test_reassembler =
        io::detail::reassembler<plexus::inproc::inproc_executor<testing::test_clock> &,
                                plexus::inproc::inproc_timer<testing::test_clock>>;

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
    r.on_deliver([&delivered](std::span<const std::byte> b) {
        delivered = std::vector<std::byte>(b.begin(), b.end());
    });

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

    int deliveries = 0;
    std::vector<std::byte> got;
    r.on_deliver([&](std::span<const std::byte> b) {
        ++deliveries;
        got.assign(b.begin(), b.end());
    });

    const auto a = seq_bytes(3, 1), b = seq_bytes(3, 50), c = seq_bytes(3, 90);
    CHECK(r.feed(7, 2, 3, c) == test_reassembler::outcome::admitted);
    CHECK(r.feed(7, 0, 3, a) == test_reassembler::outcome::admitted);
    CHECK(r.feed(7, 1, 3, b) == test_reassembler::outcome::completed);

    CHECK(deliveries == 1);
    REQUIRE(got.size() == 9);
    CHECK(std::equal(a.begin(), a.end(), got.begin()));          // reassembled in index order
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

    CHECK_FALSE(delivered);            // best-effort drop-whole: the lost-fragment message is gone
    CHECK(r.in_flight() == 0);
    CHECK(r.held_bytes() == 0);
}

TEST_CASE("reassembler rejects a new partial that would breach the total-memory cap", "[fragment][reassemble]")
{
    testing::harness h;
    const auto one_msg = 10 + test_reassembler::structural_cost(2);
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

    CHECK(r.feed(1, 5, 3, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed);   // idx >= cnt
    CHECK(r.feed(1, 0, 0, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed);   // cnt == 0
    CHECK(r.feed(1, 0, 0xFFFF, seq_bytes(4, 0)) == test_reassembler::outcome::dropped_malformed); // cnt over max
    CHECK(r.in_flight() == 0);

    // A duplicate fragment is ignored (the first wins), the message still completes once.
    int deliveries = 0;
    r.on_deliver([&](std::span<const std::byte>) { ++deliveries; });
    CHECK(r.feed(9, 0, 2, seq_bytes(3, 1)) == test_reassembler::outcome::admitted);
    CHECK(r.feed(9, 0, 2, seq_bytes(3, 9)) == test_reassembler::outcome::admitted);   // duplicate idx 0
    CHECK(r.feed(9, 1, 2, seq_bytes(3, 2)) == test_reassembler::outcome::completed);
    CHECK(deliveries == 1);
}
