#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_HANDSHAKE_FRAME_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_HANDSHAKE_FRAME_H

#include "plexus/wire/udp_envelope.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::datagram::detail {

// The UDP handshake control frame's type, carried as the inner frame under the reliable_arq
// envelope kind. The dialer retransmits hs_request on the ARQ ladder until hs_response arrives.
enum class udp_hs_type : std::uint8_t
{
    request  = 0,
    response = 1,
};

// The delivery mode a dialer declares so the acceptor mints a symmetric channel, riding the
// handshake's 2nd inner byte; a legacy 1-byte handshake (no mode byte) defaults to best_effort.
enum class udp_channel_mode : std::uint8_t
{
    best_effort       = 0,
    reliable_datagram = 1,
};

// A legacy frame without the ISN bytes decodes initial_seq = 0 (the back-compat contract).
struct udp_handshake
{
    udp_hs_type type;
    udp_channel_mode mode;
    std::uint16_t initial_seq;
};

// The inner frame is [hs_type, channel_mode, isn_lo, isn_hi]; the ISN is little-endian. The
// envelope seq is 0 (the handshake predates the dedup/ARQ sequence).
inline void encode_handshake_into(std::vector<std::byte> &out, udp_hs_type type, udp_channel_mode mode = udp_channel_mode::best_effort, std::uint16_t initial_seq = 0)
{
    const std::byte inner[4]{static_cast<std::byte>(type), static_cast<std::byte>(mode), static_cast<std::byte>(initial_seq & 0xFF), static_cast<std::byte>((initial_seq >> 8) & 0xFF)};
    wire::wrap_udp_into(out, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 4});
}

// The strict first-byte check (request=0 or response=1) keeps the handshake control space disjoint
// from the reliable-ARQ inner markers (segment=2, ack=3), so an ARQ data segment falls through to
// the ARQ demux. A 1-byte handshake decodes mode=best_effort; absent ISN bytes decode initial_seq=0.
inline std::optional<udp_handshake> decode_handshake(std::span<const std::byte> datagram)
{
    auto dec = wire::unwrap_udp(datagram);
    if(!dec || dec->kind != wire::udp_envelope_kind::reliable_arq || dec->frame.empty() || dec->frame.size() == 3 || dec->frame.size() > 4)
        return std::nullopt;
    const auto v = std::to_integer<std::uint8_t>(dec->frame[0]);
    if(v != static_cast<std::uint8_t>(udp_hs_type::request) && v != static_cast<std::uint8_t>(udp_hs_type::response))
        return std::nullopt;
    auto mode = udp_channel_mode::best_effort;
    if(dec->frame.size() >= 2)
    {
        const auto m = std::to_integer<std::uint8_t>(dec->frame[1]);
        if(m > static_cast<std::uint8_t>(udp_channel_mode::reliable_datagram))
            return std::nullopt; // unknown mode byte: fail closed
        mode = static_cast<udp_channel_mode>(m);
    }
    std::uint16_t isn = 0;
    if(dec->frame.size() == 4)
        isn = static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(dec->frame[2]) | (std::to_integer<std::uint16_t>(dec->frame[3]) << 8));
    return udp_handshake{static_cast<udp_hs_type>(v), mode, isn};
}

}

#endif
