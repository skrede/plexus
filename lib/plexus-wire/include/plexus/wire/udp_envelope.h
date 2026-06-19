#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H

#include "plexus/wire/cursor.h"

#include <cstddef>
#include <cstdint>
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

// A fragmented datagram carries a 10-byte sub-header AFTER the 3-byte envelope:
//
//   offset  size  field
//   ------  ----  ----------------------------------------------------------------
//    3       2    msg_id     uint16, big-endian. Groups the fragments of one logical
//                            message; the reassembler keys its partial-message table on it.
//    5       4    frag_idx   uint32, big-endian. This fragment's 0-based position.
//    9       4    frag_cnt   uint32, big-endian. The total fragment count for the message.
//   13       ..   bytes      this fragment's slice of the payload, passed through verbatim.
//
// frag_idx/frag_cnt are uint32 because the fragment count of one message is bounded by
// the message size over the minimum fragment payload, which can exceed the uint16 range;
// the field width must not be the limiter on how large a message may fragment. msg_id
// stays uint16 deliberately: it does NOT count fragments — it groups them, and is sized to
// exceed the bounded reassembler's in-flight table, so an id wrap cannot alias a live entry
// even at a far larger per-message fragment count. This sub-header is APPENDED only when the
// FRAGMENTED bit is set, so the common single-datagram path keeps the 3-byte overhead with
// zero fragmentation cost.
constexpr std::size_t udp_fragment_subheader       = 10;
constexpr std::size_t udp_fragment_header_overhead = udp_envelope_overhead + udp_fragment_subheader;

enum class udp_envelope_kind : std::uint8_t
{
    best_effort  = 0,
    reliable_arq = 1,
};

namespace detail {

constexpr std::uint8_t udp_kind_shift     = 6u;
constexpr std::uint8_t udp_kind_mask      = 0b1100'0000u;
constexpr std::uint8_t udp_fragmented_bit = 0b0000'0001u;

// Pack the kind discriminator into bits 7..6 and the FRAGMENTED flag into bit0. The
// flag defaults false, so the existing single-datagram wrap is byte-unchanged; bits 5..1
// stay 0 (reserved). A fragmenting send passes fragmented=true to set bit0.
inline std::uint8_t pack_ver_flags(udp_envelope_kind kind, bool fragmented = false) noexcept
{
    auto bits = static_cast<std::uint8_t>(static_cast<std::uint8_t>(kind) << udp_kind_shift);
    if(fragmented)
        bits |= udp_fragmented_bit;
    return bits;
}

}

struct udp_decode_result
{
    udp_envelope_kind          kind;
    std::uint16_t              seq;
    std::span<const std::byte> frame;
    bool                       fragmented;
};

inline std::vector<std::byte> wrap_udp(udp_envelope_kind kind, std::uint16_t seq,
                                       std::span<const std::byte> frame)
{
    std::vector<std::byte> buf(udp_envelope_overhead + frame.size());
    writer                 w{buf};

    w.u8(detail::pack_ver_flags(kind));
    w.u16(seq);
    w.bytes(frame);

    return buf;
}

// Wrap into a caller-owned buffer reused across calls. resize() reuses the
// buffer's capacity, so the steady-state send loop allocates nothing after the
// warm-up grow (the allocating overload above stays for one-shot callers).
inline void wrap_udp_into(std::vector<std::byte> &out, udp_envelope_kind kind, std::uint16_t seq,
                          std::span<const std::byte> frame)
{
    out.resize(udp_envelope_overhead + frame.size());
    writer w{out};

    w.u8(detail::pack_ver_flags(kind));
    w.u16(seq);
    w.bytes(frame);
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

    reader     r{datagram};
    const auto ver_flags = r.u8();
    const auto seq       = r.u16();

    return udp_decode_result{.kind = static_cast<udp_envelope_kind>(
                                     (ver_flags & detail::udp_kind_mask) >> detail::udp_kind_shift),
                             .seq        = seq,
                             .frame      = r.rest(),
                             .fragmented = (ver_flags & detail::udp_fragmented_bit) != 0u};
}

// Wrap an inner frame with the FRAGMENTED bit set but WITHOUT the 6-byte wire
// sub-header: the reliable-ARQ fragment path carries its msg_id/index/count INSIDE the
// ARQ segment payload (so each fragment is one independently-ARQ'd segment), and only
// needs the envelope bit to flag the peer that the in-order-delivered payload is a
// fragment to reassemble. The best_effort path uses wrap_udp_fragment_into (the bit AND
// the sub-header) instead; the unfragmented wrap_udp_into above is untouched.
inline void wrap_udp_into_fragmented(std::vector<std::byte> &out, udp_envelope_kind kind,
                                     std::uint16_t seq, std::span<const std::byte> frame)
{
    out.resize(udp_envelope_overhead + frame.size());
    writer w{out};

    w.u8(detail::pack_ver_flags(kind, true));
    w.u16(seq);
    w.bytes(frame);
}

// The decoded fragment sub-header: the message-grouping id, this fragment's index and
// the total count, plus the fragment's own payload slice (a view into the caller's
// frame). The reassembler keys its table on these fields.
struct udp_fragment_header
{
    std::uint16_t              msg_id;
    std::uint32_t              frag_idx;
    std::uint32_t              frag_cnt;
    std::span<const std::byte> payload;
};

// Wrap one fragment into a caller-owned buffer reused across sends: the 3-byte envelope
// with the FRAGMENTED bit set, the 10-byte fragment sub-header, then the fragment bytes.
// resize() reuses the buffer's capacity so a fragmenting send loop allocates nothing
// after the warm-up grow. The unfragmented wrap_udp_into above is untouched — this
// overhead exists only on the fragmenting path.
inline void wrap_udp_fragment_into(std::vector<std::byte> &out, udp_envelope_kind kind,
                                   std::uint16_t seq, std::uint16_t msg_id, std::uint32_t frag_idx,
                                   std::uint32_t frag_cnt, std::span<const std::byte> frag_bytes)
{
    out.resize(udp_fragment_header_overhead + frag_bytes.size());
    writer w{out};

    w.u8(detail::pack_ver_flags(kind, true));
    w.u16(seq);
    w.u16(msg_id);
    w.u32(frag_idx);
    w.u32(frag_cnt);
    w.bytes(frag_bytes);
}

// Encode the BARE fragment sub-header + slice — [msg_id:2][frag_idx:4][frag_cnt:4][slice]
// — with no envelope. This is the reliable-ARQ fragment payload: the fragment rides as
// one ARQ segment whose in-order delivery the peer decodes with decode_udp_fragment_header
// (the layout is identical to the wire sub-header). Reused caller buffer, no per-send alloc.
inline void encode_udp_fragment_payload_into(std::vector<std::byte> &out, std::uint16_t msg_id,
                                             std::uint32_t frag_idx, std::uint32_t frag_cnt,
                                             std::span<const std::byte> slice)
{
    out.resize(udp_fragment_subheader + slice.size());
    writer w{out};

    w.u16(msg_id);
    w.u32(frag_idx);
    w.u32(frag_cnt);
    w.bytes(slice);
}

// Decode the fragment sub-header from an untrusted inner frame (the .frame an unwrap
// of a fragmented datagram yields). Fail-closed: a frame shorter than the 10-byte
// sub-header is rejected to nullopt before any read, never indexing past the span. The
// returned payload is a view into the caller's frame (frame.subspan past the sub-header).
inline std::optional<udp_fragment_header>
decode_udp_fragment_header(std::span<const std::byte> frame)
{
    if(frame.size() < udp_fragment_subheader)
        return std::nullopt;

    reader r{frame};
    auto   msg_id   = r.u16();
    auto   frag_idx = r.u32();
    auto   frag_cnt = r.u32();
    return udp_fragment_header{
            .msg_id = msg_id, .frag_idx = frag_idx, .frag_cnt = frag_cnt, .payload = r.rest()};
}

}

#endif
