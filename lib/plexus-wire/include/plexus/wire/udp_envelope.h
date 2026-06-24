#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_ENVELOPE_H

#include "plexus/wire/cursor.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The UDP outer wrapper around one opaque inner frame: a datagram-only envelope with no length
// prefix (the datagram boundary is the framing) and no MAC. The inner frame is passed through
// verbatim and never inspected.
//
//   offset  size  field
//   ------  ----  ----------------------------------------------------------------
//    0       1    ver_flags  bit7..6 = envelope kind (0 = best_effort, 1 = reliable
//                            ARQ); bit0 = FRAGMENTED (reserved, always 0); bits 5..1
//                            reserved 0.
//    1       2    seq        uint16, big-endian. best_effort: the dedup seq. reliable:
//                            the sliding-window sequence number (wrap-safe to 2^15).
//    3       ..   frame      the opaque inner frame, passed through verbatim.
//
// The ver_flags kind discriminator is the seam a future authenticated/encrypted datagram path
// keys on to bypass the dedup + ARQ engine. Reserved here; nothing is built.
constexpr std::size_t udp_envelope_overhead = 3;

// A fragmented datagram carries a 10-byte sub-header AFTER the 3-byte envelope:
//
//   offset  size  field
//   ------  ----  ----------------------------------------------------------------
//    3       2    msg_id     uint16, big-endian. Groups the fragments of one message.
//    5       4    frag_idx   uint32, big-endian. This fragment's 0-based position.
//    9       4    frag_cnt   uint32, big-endian. The total fragment count.
//   13       ..   bytes      this fragment's slice, passed through verbatim.
//
// frag_idx/frag_cnt are uint32: the fragment count can exceed the uint16 range, so the field
// width must not limit how large a message may fragment. msg_id stays uint16 — it groups, not
// counts, and is sized to exceed the bounded reassembler's in-flight table so an id wrap cannot
// alias a live entry. The sub-header is APPENDED only when the FRAGMENTED bit is set.
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

// Pack the kind discriminator into bits 7..6 and the FRAGMENTED flag into bit0 (bits 5..1 stay
// reserved 0).
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
    udp_envelope_kind kind;
    std::uint16_t seq;
    std::span<const std::byte> frame;
    bool fragmented;
};

// The decoded fragment sub-header plus the fragment's payload slice (a view into the caller's
// frame).
struct udp_fragment_header
{
    std::uint16_t msg_id;
    std::uint32_t frag_idx;
    std::uint32_t frag_cnt;
    std::span<const std::byte> payload;
};

}

#include "plexus/wire/detail/udp_envelope_codec.h"

#endif
