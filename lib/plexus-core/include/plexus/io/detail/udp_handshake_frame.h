#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UDP_HANDSHAKE_FRAME_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UDP_HANDSHAKE_FRAME_H

#include "plexus/wire/udp_envelope.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io::detail {

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

// The per-channel delivery mode a dialer declares in the handshake so the acceptor mints
// a symmetric channel: best_effort (fire-and-forget UDP, the "udp" scheme) or
// reliable_datagram (the in-order ARQ, the "udpr" scheme). It rides the handshake's 2nd
// inner byte; a legacy 1-byte handshake (no mode byte) defaults to best_effort.
enum class udp_channel_mode : std::uint8_t
{
    best_effort = 0,
    reliable_datagram = 1,
};

// A decoded handshake: the request/response type, the dialer-declared channel mode, and
// the advertised per-session initial sequence number (RFC 6528 lineage). A legacy frame
// without the ISN bytes decodes initial_seq = 0 (the documented back-compat contract — the
// 0-on-both-ends seam).
struct udp_handshake
{
    udp_hs_type      type;
    udp_channel_mode mode;
    std::uint16_t    initial_seq;
};

// Encode a handshake control frame into a caller-owned reused buffer (no per-send
// alloc after warm-up). seq is 0: the handshake predates the dedup/ARQ sequence. The
// inner frame is [hs_type, channel_mode, isn_lo, isn_hi] so the acceptor learns the class
// AND the peer's per-session ISN; the ISN is little-endian.
inline void encode_handshake_into(std::vector<std::byte> &out, udp_hs_type type,
                                  udp_channel_mode mode = udp_channel_mode::best_effort,
                                  std::uint16_t initial_seq = 0)
{
    const std::byte inner[4]{static_cast<std::byte>(type), static_cast<std::byte>(mode),
                             static_cast<std::byte>(initial_seq & 0xFF),
                             static_cast<std::byte>((initial_seq >> 8) & 0xFF)};
    wire::wrap_udp_into(out, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 4});
}

// Recognize a handshake control frame: a reliable_arq datagram whose inner frame is the
// [hs_type] (legacy), [hs_type, channel_mode], or [hs_type, channel_mode, isn_lo, isn_hi]
// control AND whose FIRST byte is a VALID hs_type (request=0 or response=1). The strict
// first-byte check keeps the handshake control space disjoint from the reliable-ARQ inner
// markers (segment=2, ack=3): a 1-byte ARQ data segment with an empty payload is [2], which
// is NOT a handshake and falls through to the ARQ demux. A 1-byte handshake decodes
// mode=best_effort; absent ISN bytes decode initial_seq=0 (back-compat). Anything else
// (real reliable data / an ack) returns nullopt.
inline std::optional<udp_handshake> decode_handshake(std::span<const std::byte> datagram)
{
    auto dec = wire::unwrap_udp(datagram);
    if(!dec || dec->kind != wire::udp_envelope_kind::reliable_arq
       || dec->frame.empty() || dec->frame.size() == 3 || dec->frame.size() > 4)
        return std::nullopt;
    const auto v = std::to_integer<std::uint8_t>(dec->frame[0]);
    if(v != static_cast<std::uint8_t>(udp_hs_type::request) && v != static_cast<std::uint8_t>(udp_hs_type::response))
        return std::nullopt;
    auto mode = udp_channel_mode::best_effort;
    if(dec->frame.size() >= 2)
    {
        const auto m = std::to_integer<std::uint8_t>(dec->frame[1]);
        if(m > static_cast<std::uint8_t>(udp_channel_mode::reliable_datagram))
            return std::nullopt;                 // unknown mode byte: fail closed
        mode = static_cast<udp_channel_mode>(m);
    }
    std::uint16_t isn = 0;
    if(dec->frame.size() == 4)
        isn = static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(dec->frame[2])
                                         | (std::to_integer<std::uint16_t>(dec->frame[3]) << 8));
    return udp_handshake{static_cast<udp_hs_type>(v), mode, isn};
}

}

#endif
