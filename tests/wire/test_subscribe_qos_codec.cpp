#include "plexus/wire/subscribe.h"
#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

using namespace plexus::wire;

namespace {

// A non-default region: durability=none, delivery=pull, replay_depth=7, both
// request flags set, and distinct non-zero deadline/lease/priority values so a
// round-trip proves EVERY field survives.
subscribe_qos_region non_default_region()
{
    return subscribe_qos_region{.durability                  = 0, // none
                                .delivery_mode               = 1, // pull
                                .replay_depth                = 7,
                                .requested_flags             = detail::k_qos_flag_requires_source_identity | detail::k_qos_flag_requested_reliable,
                                .requested_deadline_ns       = 0xA1B2C3D4E5F60789ULL,
                                .requested_lease_ns          = 0x0102030405060708ULL,
                                .requested_priority          = 5,
                                .requested_max_message_bytes = 0x00ABCDEFu};
}

subscribe_request base_request()
{
    return subscribe_request{
            .fqn = "robot/state", .type_name = "geometry/Pose", .topic_hash = 0xDEADBEEFCAFEF00DULL, .type_hash = 0x1122334455667788ULL, .source = endpoint_source_type::publisher};
}

}

TEST_CASE("subscribe_qos region round-trips every field", "[wire][subscribe_qos]")
{
    auto req    = base_request();
    req.has_qos = true;
    req.qos     = non_default_region();

    auto bytes   = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(bytes);

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->has_qos);
    CHECK(decoded->fqn == req.fqn);
    CHECK(decoded->type_name == req.type_name);
    CHECK(decoded->topic_hash == req.topic_hash);
    CHECK(decoded->type_hash == req.type_hash);
    CHECK(decoded->qos.durability == 0);
    CHECK(decoded->qos.delivery_mode == 1);
    CHECK(decoded->qos.replay_depth == 7);
    CHECK(decoded->qos.requested_flags == req.qos.requested_flags);
    CHECK(decoded->qos.requested_deadline_ns == req.qos.requested_deadline_ns);
    CHECK(decoded->qos.requested_lease_ns == req.qos.requested_lease_ns);
    CHECK(decoded->qos.requested_priority == 5);
    CHECK(decoded->qos.requested_max_message_bytes == req.qos.requested_max_message_bytes);
}

TEST_CASE("has_qos=false encode is byte-identical to the pre-region encoding", "[wire][subscribe_qos]")
{
    auto req = base_request();
    // The region is set but the flag is CLEAR: the encode must write NO trailing
    // bytes, so the result is the exact pre-region layout a v3 producer wrote.
    req.has_qos = false;
    req.qos     = non_default_region();

    auto with_struct = encode_subscribe_request(req);

    // The reference: the same request with a default-constructed region and the
    // flag clear. Both must be byte-for-byte equal — the region never leaks.
    subscribe_request plain = base_request();
    auto reference          = encode_subscribe_request(plain);

    CHECK(with_struct == reference);
    // And the encoded size is the pre-region minimum-plus-strings (no +32).
    CHECK(with_struct.size() == detail::subscribe_request_fixed_prefix + 2 + req.fqn.size() + 2 + req.type_name.size());
}

TEST_CASE("an absent region decodes back to the friendly defaults", "[wire][subscribe_qos]")
{
    auto req    = base_request();
    req.has_qos = false;
    auto bytes  = encode_subscribe_request(req);

    auto decoded = decode_subscribe_request(bytes);
    REQUIRE(decoded.has_value());
    CHECK_FALSE(decoded->has_qos);
    // The defaulted wire region: durability=0 maps to none on the wire, but the
    // CORE lift treats !has_qos as the io::subscriber_qos friendly default
    // (latest). Here we only assert the wire struct is the value-initialized POD.
    CHECK(decoded->qos.replay_depth == 0);
    CHECK(decoded->qos.requested_flags == 0);
}

TEST_CASE("a truncated QoS region decodes to nullopt", "[wire][subscribe_qos]")
{
    auto req    = base_request();
    req.has_qos = true;
    req.qos     = non_default_region();
    auto bytes  = encode_subscribe_request(req);

    // Drop the last byte of the fixed region: the declared length prefix now
    // over-runs the buffer, so read_length_prefixed refuses -> nullopt.
    bytes.pop_back();
    auto decoded = decode_subscribe_request(bytes);
    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("a region whose declared length is not exactly 30 decodes to nullopt", "[wire][subscribe_qos]")
{
    auto req    = base_request();
    req.has_qos = true;
    req.qos     = non_default_region();
    auto bytes  = encode_subscribe_request(req);

    // Locate the region length prefix: it sits right after fqn + type_name, i.e.
    // at fixed_prefix + 2 + fqn + 2 + type_name. Rewrite it to a wrong (in-range
    // but != 30) value while leaving 30 bytes present -> exact-length check fails.
    const std::size_t prefix_off = detail::subscribe_request_fixed_prefix + 2 + req.fqn.size() + 2 + req.type_name.size();
    REQUIRE(prefix_off + 2 <= bytes.size());
    detail::write_u16(bytes.data() + prefix_off, 29); // claim 29, not 30
    // Shorten the trailing payload to match the smaller claim so the buffer is
    // self-consistent (the length read succeeds) but the exact-26 gate rejects it.
    bytes.pop_back();
    auto decoded = decode_subscribe_request(bytes);
    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("an over-cap region length decodes to nullopt", "[wire][subscribe_qos]")
{
    // A hand-built frame whose region length prefix claims more than k_max_qos_region.
    auto req   = base_request();
    auto bytes = encode_subscribe_request(req); // no region

    // Append a uint16_t length prefix claiming a huge region, with no payload: the
    // read_length_prefixed bounds gate refuses (the payload does not fit), so the
    // decode is nullopt either way — and even if bytes were present, the >cap and
    // !=26 checks would refuse.
    const auto base_size = bytes.size();
    bytes.resize(base_size + 2);
    detail::write_u16(bytes.data() + base_size, static_cast<uint16_t>(detail::k_max_qos_region + 100));
    auto decoded = decode_subscribe_request(bytes);
    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("the protocol version is at 7 and the region is trivially copyable", "[wire][subscribe_qos]")
{
    static_assert(k_protocol_version == 7);
    static_assert(detail::k_qos_region_size == 30);
    static_assert(std::is_trivially_copyable_v<subscribe_qos_region>);
    CHECK(k_protocol_version == 7);
}

TEST_CASE("the typed-strict flag bit round-trips in the flags byte", "[wire][subscribe_qos]")
{
    auto req    = base_request();
    req.has_qos = true;
    req.qos     = non_default_region();
    req.qos.requested_flags |= detail::k_qos_flag_typed_strict;

    auto bytes   = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(bytes);

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->has_qos);
    CHECK((decoded->qos.requested_flags & detail::k_qos_flag_typed_strict) != 0);
}

TEST_CASE("the type_undeclared status round-trips through the response codec", "[wire][subscribe_qos]")
{
    subscribe_response resp{.topic_hash = 0x1122334455667788ULL, .status = subscribe_status::type_undeclared};

    auto bytes   = encode_subscribe_response(resp);
    auto decoded = decode_subscribe_response(bytes);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == resp.topic_hash);
    CHECK(decoded->status == subscribe_status::type_undeclared);
    CHECK_FALSE(decoded->has_degraded);
}
