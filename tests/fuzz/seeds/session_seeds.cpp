#include "seed_sink.h"

#include "plexus/wire/handshake.h"

#include <cstdint>
#include <string_view>

namespace {

constexpr std::uint8_t k_drive     = 0;
constexpr std::uint8_t k_inject    = 1;
constexpr std::uint8_t k_raw       = 2;
constexpr std::uint8_t k_unknown   = 3;
constexpr std::uint8_t k_subscribe = 4;
constexpr std::uint8_t k_declare   = 5;
constexpr std::uint8_t k_heartbeat = 6;
constexpr std::uint8_t k_tear_down = 7;

constexpr std::uint8_t k_requester = 0;
constexpr std::uint8_t k_responder = 1;

constexpr std::uint8_t k_handshake_frame   = 0;
constexpr std::uint8_t k_heartbeat_frame   = 1;
constexpr std::uint8_t k_subscribe_frame   = 2;
constexpr std::uint8_t k_data_frame        = 3;
constexpr std::uint8_t k_declaration_frame = 4;

constexpr std::string_view k_topic = "/plexus/chatter";

void field(seed &out, std::string_view s)
{
    append(out, {lo(s.size())});
    append(out, text(s));
}

void header(seed &out, std::uint8_t session_id)
{
    if(session_id == 0)
        append(out, {0, 0});
    else
        append(out, {0, 1, 1, session_id});
}

void handshake(seed &out, std::uint8_t to, bool response, std::uint8_t from_id)
{
    append(out, {k_inject, to, k_handshake_frame, response ? std::uint8_t{1} : std::uint8_t{0}, from_id, 1, 0, 1, 0, plexus::wire::k_protocol_version, 0, 0});
    if(response)
        append(out, {static_cast<std::uint8_t>(plexus::wire::handshake_status::accepted)});
    header(out, 0);
}

void data(seed &out, std::uint8_t to, std::uint8_t session_id)
{
    append(out, {k_inject, to, k_data_frame, 1, 1, 1});
    field(out, k_topic);
    append(out, {0});
    field(out, "hello");
    header(out, session_id);
}

void declaration(seed &out)
{
    field(out, k_topic);
    field(out, "std_msgs/String");
    append(out, {0, 1, 5, 2});
}

seed established_traffic()
{
    seed out{std::byte{k_drive}, std::byte{k_subscribe}, std::byte{k_requester}};
    field(out, k_topic);
    append(out, {k_drive});
    data(out, k_requester, 1);
    append(out, {k_drive, k_heartbeat, k_responder, k_drive, k_tear_down, k_requester, k_drive});
    return out;
}

seed declared_traffic()
{
    seed out{std::byte{k_drive}, std::byte{k_declare}, std::byte{k_requester}};
    declaration(out);
    append(out, {k_drive, k_inject, k_responder, k_declaration_frame});
    declaration(out);
    header(out, 0);
    for(const std::uint8_t session_id : {5, 6})
    {
        append(out, {k_inject, k_responder, k_heartbeat_frame, 1, 1});
        header(out, session_id);
    }
    append(out, {k_drive});
    return out;
}

seed subscribe_then_garbage()
{
    seed out{std::byte{k_drive}, std::byte{k_inject}, std::byte{k_responder}, std::byte{k_subscribe_frame}, std::byte{0}};
    field(out, k_topic);
    field(out, "");
    append(out, {0, 0, 1});
    header(out, 0);
    append(out, {k_drive, k_raw, k_requester, 4, 'a', 'b', 'c', 'd', k_drive});
    return out;
}

seed unknown_type_when_established()
{
    seed out{std::byte{k_drive}, std::byte{k_unknown}, std::byte{k_responder}, std::byte{0x30}};
    field(out, "x");
    header(out, 9);
    append(out, {k_drive});
    return out;
}

seed handshake_before_drive(std::uint8_t to, bool response, std::uint8_t from_id)
{
    seed out;
    handshake(out, to, response, from_id);
    append(out, {k_drive});
    return out;
}

}

// Grammar: one operation byte (modulo eight) and its fields, repeated; the side byte's low bit picks the responder.
seed_set session_sequence_seeds()
{
    return {seed{std::byte{k_drive}},
            seed{std::byte{k_drive}, std::byte{k_tear_down}, std::byte{k_requester}},
            seed{std::byte{k_drive}, std::byte{k_tear_down}, std::byte{k_responder}, std::byte{k_drive}},
            handshake_before_drive(k_responder, false, 0x07),
            handshake_before_drive(k_requester, true, 0x01),
            established_traffic(),
            declared_traffic(),
            subscribe_then_garbage(),
            unknown_type_when_established()};
}
