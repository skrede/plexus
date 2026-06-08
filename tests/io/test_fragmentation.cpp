#include "plexus/io/fragmentation.h"
#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

using namespace plexus;

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
    std::uint16_t idx;
    std::uint16_t cnt;
    std::vector<std::byte> bytes;
};

std::vector<recorded_fragment> collect(std::span<const std::byte> payload, std::size_t budget)
{
    std::vector<recorded_fragment> seen;
    io::fragment_sink sink = [&seen](std::uint16_t idx, std::uint16_t cnt, std::span<const std::byte> b) {
        seen.push_back({idx, cnt, std::vector<std::byte>(b.begin(), b.end())});
    };
    const auto returned = io::split(payload, budget, /*msg_id*/ 1, sink);
    REQUIRE(returned == seen.size());
    return seen;
}

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
