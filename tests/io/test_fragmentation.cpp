#include "test_fragmentation_common.h"

using namespace fragmentation_fixture;

TEST_CASE("wire fragment header round-trips msg_id/frag_idx/frag_cnt and bytes", "[fragment][wire]")
{
    const auto             payload = bytes_of({0xDE, 0xAD, 0xBE, 0xEF, 0x10});
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
    auto                                                    frag = wire::decode_udp_fragment_header(std::span<const std::byte>{truncated});
    CHECK_FALSE(frag.has_value());

    std::array<std::byte, 0> empty{};
    CHECK_FALSE(wire::decode_udp_fragment_header(std::span<const std::byte>{empty}).has_value());
}

TEST_CASE("the unfragmented wrap path keeps the 3-byte envelope unchanged", "[fragment][wire]")
{
    const auto             frame = bytes_of({1, 2, 3, 4});
    std::vector<std::byte> out;
    wire::wrap_udp_into(out, wire::udp_envelope_kind::best_effort, 42, frame);

    REQUIRE(out.size() == wire::udp_envelope_overhead + frame.size());
    auto env = wire::unwrap_udp(out);
    REQUIRE(env.has_value());
    CHECK_FALSE(env->fragmented);
}
