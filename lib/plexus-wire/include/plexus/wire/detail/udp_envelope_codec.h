#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_UDP_ENVELOPE_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_UDP_ENVELOPE_CODEC_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/udp_envelope.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

inline std::vector<std::byte> wrap_udp(udp_envelope_kind kind, std::uint16_t seq, std::span<const std::byte> frame)
{
    std::vector<std::byte> buf(udp_envelope_overhead + frame.size());
    writer                 w{buf};
    w.u8(detail::pack_ver_flags(kind));
    w.u16(seq);
    w.bytes(frame);
    return buf;
}

// Wrap into a caller-owned buffer reused across calls. resize() reuses the buffer's capacity, so
// the steady-state send loop allocates nothing after the warm-up grow.
inline void wrap_udp_into(std::vector<std::byte> &out, udp_envelope_kind kind, std::uint16_t seq, std::span<const std::byte> frame)
{
    out.resize(udp_envelope_overhead + frame.size());
    writer w{out};
    w.u8(detail::pack_ver_flags(kind));
    w.u16(seq);
    w.bytes(frame);
}

// Decode an untrusted datagram. A buffer shorter than the fixed overhead is rejected fail-closed.
// The reserved FRAGMENTED bit is decoded, not rejected, so a future fragmenting peer's flag is
// observable without a wire change. Reserved bits 5..1 are likewise decoded-PERMISSIVE (an unknown
// future bit is silently ignored) — the deliberate OPPOSITE of the strict handshake mode-byte
// check (a wire KIND must be exact; a flag bit must be forward-compatible).
inline std::optional<udp_decode_result> unwrap_udp(std::span<const std::byte> datagram)
{
    if(datagram.size() < udp_envelope_overhead)
        return std::nullopt;
    reader     r{datagram};
    const auto ver_flags = r.u8();
    const auto seq       = r.u16();
    return udp_decode_result{.kind       = static_cast<udp_envelope_kind>((ver_flags & detail::udp_kind_mask) >> detail::udp_kind_shift),
                             .seq        = seq,
                             .frame      = r.rest(),
                             .fragmented = (ver_flags & detail::udp_fragmented_bit) != 0u};
}

// Wrap an inner frame with the FRAGMENTED bit set but WITHOUT the 10-byte sub-header: the
// reliable-ARQ fragment path carries its msg_id/index/count INSIDE the ARQ segment payload, and
// only needs the envelope bit to flag the peer that the in-order-delivered payload is a fragment.
inline void wrap_udp_into_fragmented(std::vector<std::byte> &out, udp_envelope_kind kind, std::uint16_t seq, std::span<const std::byte> frame)
{
    out.resize(udp_envelope_overhead + frame.size());
    writer w{out};
    w.u8(detail::pack_ver_flags(kind, true));
    w.u16(seq);
    w.bytes(frame);
}

// Wrap one fragment: the 3-byte envelope with the FRAGMENTED bit set, the 10-byte fragment
// sub-header, then the fragment bytes (the best_effort fragment path). Reused caller buffer.
inline void wrap_udp_fragment_into(std::vector<std::byte> &out, udp_envelope_kind kind, std::uint16_t seq, std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt,
                                   std::span<const std::byte> frag_bytes)
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

// Encode the BARE fragment sub-header + slice — [msg_id:2][frag_idx:4][frag_cnt:4][slice] — with no
// envelope. This is the reliable-ARQ fragment payload: the fragment rides as one ARQ segment whose
// in-order delivery the peer decodes with decode_udp_fragment_header (layout identical to the wire
// sub-header). Reused caller buffer, no per-send alloc.
inline void encode_udp_fragment_payload_into(std::vector<std::byte> &out, std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt, std::span<const std::byte> slice)
{
    out.resize(udp_fragment_subheader + slice.size());
    writer w{out};
    w.u16(msg_id);
    w.u32(frag_idx);
    w.u32(frag_cnt);
    w.bytes(slice);
}

// Decode the fragment sub-header from an untrusted inner frame. Fail-closed: a frame shorter than
// the 10-byte sub-header is rejected to nullopt before any read. The returned payload is a view
// into the caller's frame (frame past the sub-header).
inline std::optional<udp_fragment_header> decode_udp_fragment_header(std::span<const std::byte> frame)
{
    if(frame.size() < udp_fragment_subheader)
        return std::nullopt;
    reader r{frame};
    auto   msg_id   = r.u16();
    auto   frag_idx = r.u32();
    auto   frag_cnt = r.u32();
    return udp_fragment_header{.msg_id = msg_id, .frag_idx = frag_idx, .frag_cnt = frag_cnt, .payload = r.rest()};
}

}

#endif
