#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_HANDSHAKE_FRAME_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_HANDSHAKE_FRAME_H

#include "plexus/wire/udp_envelope.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::asio::detail {

// The UDP handshake control frame: a single hs_type byte carried as the inner frame
// under the reliable_arq envelope kind. The dialer retransmits hs_request on the ARQ
// ladder until the acceptor's hs_response arrives. Keeping the handshake under the
// SAME envelope (kind discriminator) means ONE inbound demux path for both the
// handshake and future reliable data — and it is the SAME kind=reliable_arq seam a
// future authenticated/encrypted (DTLS) path keys on to bypass the dedup+ARQ engine
// (that path owns its own handshake retransmit and anti-replay). Design-in only.
enum class udp_hs_type : std::uint8_t
{
    request = 0,
    response = 1,
};

// Encode a handshake control frame into a caller-owned reused buffer (no per-send
// alloc after warm-up). seq is 0: the handshake predates the dedup/ARQ sequence.
inline void encode_handshake_into(std::vector<std::byte> &out, udp_hs_type type)
{
    const std::byte inner[1]{static_cast<std::byte>(type)};
    wire::wrap_udp_into(out, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 1});
}

// Recognize a handshake control frame: a reliable_arq datagram whose inner frame is
// exactly the single hs_type byte AND that byte is a VALID hs_type (request=0 or
// response=1). The strict value check keeps the handshake control space disjoint from
// the reliable-ARQ inner markers (segment=2, ack=3): a 1-byte ARQ data segment with an
// empty payload is [2], which is NOT a handshake and falls through to the ARQ demux.
// Anything else (real reliable data / an ack) returns nullopt.
inline std::optional<udp_hs_type> decode_handshake(std::span<const std::byte> datagram)
{
    auto dec = wire::unwrap_udp(datagram);
    if(!dec || dec->kind != wire::udp_envelope_kind::reliable_arq || dec->frame.size() != 1)
        return std::nullopt;
    const auto v = std::to_integer<std::uint8_t>(dec->frame[0]);
    if(v != static_cast<std::uint8_t>(udp_hs_type::request) && v != static_cast<std::uint8_t>(udp_hs_type::response))
        return std::nullopt;
    return static_cast<udp_hs_type>(v);
}

}

#endif
