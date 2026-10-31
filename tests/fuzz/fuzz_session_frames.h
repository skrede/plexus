#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_FRAMES_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_FRAMES_H

#include "fuzz_input.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace fuzz_session {

using frame = std::vector<std::byte>;
using plexus::wire::msg_type;

constexpr std::uint8_t k_first_unknown_type = static_cast<std::uint8_t>(msg_type::companion_offer) + 1;

inline std::span<const std::byte> bytes_field(fuzz_input &in, std::size_t cap)
{
    return std::as_bytes(in.take(in.u8() % cap));
}

inline std::string text_field(fuzz_input &in)
{
    const auto bytes = bytes_field(in, 32);
    return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
}

// The topic hash either matches the name beside it, as a real sender's does, or is any value at all.
inline std::uint64_t hash_field(fuzz_input &in, std::string_view fqn)
{
    return in.u8() % 2 == 0 ? plexus::wire::fqn_topic_hash(fqn) : in.any();
}

inline plexus::wire::frame_header header_field(fuzz_input &in, msg_type type)
{
    const std::uint8_t flags = in.u8();
    const std::uint64_t id   = in.u8() % 2 == 0 ? 0 : in.any();
    return {.type = type, .flags = flags, .session_id = id, .timestamp_ns = 0, .payload_len = 0};
}

inline frame framed(fuzz_input &in, msg_type type, std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(header_field(in, type), payload);
}

template<typename Handshake>
Handshake handshake_fields(fuzz_input &in)
{
    Handshake h{};
    h.id[0]                    = std::byte{in.u8()};
    h.version_major            = in.u8();
    h.version_minor            = in.u8();
    h.compatible_version_major = in.u8();
    h.compatible_version_minor = in.u8();
    h.protocol_version         = in.u8();
    h.fingerprint              = in.any();
    h.cipher_offer             = in.u8();
    h.chosen_cipher            = h.cipher_offer;
    return h;
}

inline frame handshake_frame(fuzz_input &in)
{
    if(in.u8() % 2 == 0)
        return framed(in, msg_type::handshake_req, plexus::wire::encode_handshake_request(handshake_fields<plexus::wire::handshake_request>(in)));
    auto response   = handshake_fields<plexus::wire::handshake_response>(in);
    response.status = static_cast<plexus::wire::handshake_status>(in.u8());
    return framed(in, msg_type::handshake_resp, plexus::wire::encode_handshake_response(response));
}

inline frame heartbeat_frame(fuzz_input &in)
{
    const plexus::wire::heartbeat hb{.version = in.u8(), .reserved = in.u8()};
    return framed(in, msg_type::heartbeat, plexus::wire::encode_heartbeat(hb));
}

inline plexus::wire::subscribe_qos_region qos_field(fuzz_input &in)
{
    return {.durability                  = in.u8(),
            .delivery_mode               = in.u8(),
            .replay_depth                = static_cast<std::uint32_t>(in.uint(4)),
            .requested_flags             = in.u8(),
            .requested_deadline_ns       = in.any(),
            .requested_lease_ns          = in.any(),
            .requested_priority          = in.u8(),
            .requested_max_message_bytes = static_cast<std::uint32_t>(in.uint(4))};
}

inline frame subscribe_request_frame(fuzz_input &in, std::uint8_t shape)
{
    plexus::wire::subscribe_request req{};
    req.fqn           = text_field(in);
    req.type_name     = text_field(in);
    req.type_declared = (shape & 0x02) != 0;
    req.topic_hash    = hash_field(in, req.fqn);
    req.type_hash     = in.any();
    req.source        = static_cast<plexus::wire::endpoint_source_type>(in.u8());
    req.has_qos       = (shape & 0x04) != 0;
    if(req.has_qos)
        req.qos = qos_field(in);
    return framed(in, msg_type::subscribe, plexus::wire::encode_subscribe_request(req));
}

inline frame subscribe_frame(fuzz_input &in)
{
    const std::uint8_t shape = in.u8();
    if(shape % 2 == 0)
        return subscribe_request_frame(in, shape);
    const plexus::wire::subscribe_response response{.topic_hash     = hash_field(in, text_field(in)),
                                                    .status         = static_cast<plexus::wire::subscribe_status>(in.u8()),
                                                    .offer          = static_cast<plexus::wire::offer_state>(in.u8()),
                                                    .has_degraded   = (shape & 0x02) != 0,
                                                    .degraded_flags = in.u8()};
    return framed(in, msg_type::subscribe_response, plexus::wire::encode_subscribe_response(response));
}

inline frame data_frame(fuzz_input &in)
{
    const auto source                    = static_cast<plexus::wire::endpoint_source_type>(in.u8());
    const std::uint64_t sequence         = in.any();
    const std::uint64_t topic_hash       = hash_field(in, text_field(in));
    const auto payload                   = bytes_field(in, 64);
    const plexus::wire::frame_header hdr = header_field(in, msg_type::unidirectional);
    frame out;
    plexus::wire::encode_unidirectional_frame_into(out, hdr, {.source = source, .sequence = sequence, .topic_hash = topic_hash}, payload);
    return out;
}

inline plexus::wire::topic_declaration declaration_field(fuzz_input &in)
{
    plexus::wire::topic_declaration td{};
    td.fqn        = text_field(in);
    td.type_name  = text_field(in);
    td.topic_hash = hash_field(in, td.fqn);
    td.type_id    = in.any();
    td.state      = static_cast<plexus::wire::type_state>(in.u8() % 4);
    return td;
}

inline frame declaration_frame(fuzz_input &in)
{
    return framed(in, msg_type::declare, plexus::wire::encode_topic_declaration(declaration_field(in)));
}

// Every byte from the first value past the enum up to 0xFF, and zero, which the enum also leaves out.
inline frame unknown_frame(fuzz_input &in)
{
    const auto type    = static_cast<msg_type>(static_cast<std::uint8_t>(k_first_unknown_type + in.u8() % (0x101 - k_first_unknown_type)));
    const auto payload = bytes_field(in, 64);
    return framed(in, type, payload);
}

}

#endif
