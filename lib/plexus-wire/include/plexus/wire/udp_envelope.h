#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H

#include "plexus/wire/byte_order.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

namespace plexus::wire {

// The UDP outer wrapper around one opaque inner frame. Unlike the stream frame
// header this is a datagram-only envelope: a published message maps to exactly
// one datagram, so there is no length prefix (the datagram boundary is the
// framing) and no MAC (the authenticated path rides a future encrypted transport,
// not this cleartext-equivalent envelope). Serializer-agnostic: the inner frame
// is passed through verbatim and never inspected.
//
//   offset  size  field
//   ------  ----  ----------------------------------------------------------------
//    0       1    ver_flags  bit7..6 = envelope kind (0 = best_effort, 1 = reliable
//                            ARQ); bit0 = FRAGMENTED (reserved, always 0 — fragment
//                            index/count append later WITHOUT reshape); bits 5..1
//                            reserved 0.
//    1       2    seq        uint16, big-endian. best_effort: the dedup seq.
//                            reliable: the sliding-window sequence number. Sized for
//                            the reliable window (wrap-safe to a 2^15 window), wide
//                            enough that best_effort dedup keeps working unchanged.
//                            This is new-on-the-UDP-envelope only — it does NOT
//                            touch the stream frame header and is NOT a protocol bump.
//    3       ..   frame      the opaque inner frame, passed through verbatim.
//
// The ver_flags kind discriminator is the seam a future authenticated/encrypted
// datagram path keys on to bypass the dedup + ARQ engine entirely (that path owns
// its own anti-replay and handshake retransmit). Reserved here; nothing is built.
constexpr std::size_t udp_envelope_overhead = 3;

enum class udp_envelope_kind : std::uint8_t
{
    best_effort = 0,
    reliable_arq = 1,
};

namespace detail {

constexpr std::uint8_t udp_kind_shift = 6u;
constexpr std::uint8_t udp_kind_mask = 0b1100'0000u;
constexpr std::uint8_t udp_fragmented_bit = 0b0000'0001u;

inline std::uint8_t pack_ver_flags(udp_envelope_kind kind) noexcept
{
    // FRAGMENTED (bit0) and bits 5..1 stay 0 this phase — reserved.
    return static_cast<std::uint8_t>(static_cast<std::uint8_t>(kind) << udp_kind_shift);
}

}

struct udp_decode_result
{
    udp_envelope_kind kind;
    std::uint16_t seq;
    std::span<const std::byte> frame;
    bool fragmented;
};

inline std::vector<std::byte> wrap_udp(udp_envelope_kind kind, std::uint16_t seq, std::span<const std::byte> frame)
{
    std::vector<std::byte> buf(udp_envelope_overhead + frame.size());
    auto *p = buf.data();

    detail::write_u8(p, detail::pack_ver_flags(kind));
    detail::write_u16(p + 1, seq);

    if(!frame.empty())
        std::memcpy(p + udp_envelope_overhead, frame.data(), frame.size());

    return buf;
}

// Wrap into a caller-owned buffer reused across calls. resize() reuses the
// buffer's capacity, so the steady-state send loop allocates nothing after the
// warm-up grow (the allocating overload above stays for one-shot callers).
inline void wrap_udp_into(std::vector<std::byte> &out, udp_envelope_kind kind, std::uint16_t seq,
                          std::span<const std::byte> frame)
{
    out.resize(udp_envelope_overhead + frame.size());
    auto *p = out.data();

    detail::write_u8(p, detail::pack_ver_flags(kind));
    detail::write_u16(p + 1, seq);

    if(!frame.empty())
        std::memcpy(p + udp_envelope_overhead, frame.data(), frame.size());
}

// Decode an untrusted datagram. A buffer shorter than the fixed overhead is
// rejected fail-closed (never index past the span). The reserved FRAGMENTED bit
// is decoded, not rejected, so a future fragmenting peer's flag is observable
// without a wire change; this phase never sets it on encode. Reserved bits 5..1 are
// likewise decoded-PERMISSIVE by design (an unknown future bit is silently ignored,
// never fail-closed) — forward-compat for a later flag, deliberately the OPPOSITE of
// the strict mode-byte check on the handshake frame (a wire KIND must be exact; a flag
// bit must be forward-compatible).
inline std::optional<udp_decode_result> unwrap_udp(std::span<const std::byte> datagram)
{
    if(datagram.size() < udp_envelope_overhead)
        return std::nullopt;

    auto *p = datagram.data();
    const auto ver_flags = detail::read_u8(p);

    return udp_decode_result{
            .kind       = static_cast<udp_envelope_kind>((ver_flags & detail::udp_kind_mask) >> detail::udp_kind_shift),
            .seq        = detail::read_u16(p + 1),
            .frame      = datagram.subspan(udp_envelope_overhead),
            .fragmented = (ver_flags & detail::udp_fragmented_bit) != 0u
    };
}

}

#endif
