#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_ACK_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_ACK_H

#include "plexus/wire/cursor.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The reliable-ARQ inner control discriminator, a LEADING control byte so ONE inbound demux
// path handles both a data segment and an ack frame. The marker values start at 2, disjoint from
// the handshake control bytes (request=0, response=1), so the three inner-frame families never
// alias: a handshake frame is exactly [0|1], a data segment is [segment][payload..], an ack is
// [ack][cumulative:2][hole-bitmap..]. The segment's seq lives in the OUTER udp_envelope.
enum class udp_arq_kind : std::uint8_t
{
    segment = 2,
    ack     = 3,
};

// A cumulative + selective acknowledgment. `cumulative` is the highest in-order seq delivered
// (expected - 1, the left edge the sender's window may slide to); in the selective bitmap, bit i
// set means (cumulative + 2 + i) was received out of order and need not be resent.
struct udp_ack
{
    // Holes beyond bitmap_bits are not nameable in the selective bitmap and fall back to
    // per-segment RTO-driven retransmit (idempotent at the receiver). The reliable ARQ binds its
    // window's upper sweep bound to this constant via a static_assert (see udp_reliable_arq.h).
    static constexpr std::size_t bitmap_bytes = 32; // 256 holes named per ack
    static constexpr std::size_t bitmap_bits  = bitmap_bytes * 8;

    std::uint16_t cumulative{0};
    std::array<std::uint8_t, bitmap_bytes> selective{}; // bit i -> (cumulative+2+i) received

    bool hole_received(std::size_t i) const noexcept
    {
        return i < bitmap_bits && (selective[i >> 3] & (1u << (i & 7u))) != 0u;
    }

    void mark_hole(std::size_t i) noexcept
    {
        if(i < bitmap_bits)
            selective[i >> 3] |= static_cast<std::uint8_t>(1u << (i & 7u));
    }
};

// The inner ack control frame on the wire: [ack-marker:1][cumulative:2][bitmap:32].
constexpr std::size_t udp_ack_frame_size = 1 + 2 + udp_ack::bitmap_bytes;

// Encode an ack into a caller-owned buffer reused across sends.
inline void encode_udp_ack_into(std::vector<std::byte> &out, const udp_ack &ack)
{
    out.resize(udp_ack_frame_size);
    writer w{out};

    w.u8(static_cast<std::uint8_t>(udp_arq_kind::ack));
    w.u16(ack.cumulative);
    w.bytes(std::span<const std::byte>{reinterpret_cast<const std::byte *>(ack.selective.data()), udp_ack::bitmap_bytes});
}

// Decode an untrusted ack control frame, fail-closed: a frame not exactly the fixed ack size, or
// whose leading marker is not the ack discriminator, decodes to nullopt.
inline std::optional<udp_ack> decode_udp_ack(std::span<const std::byte> frame)
{
    if(frame.size() != udp_ack_frame_size)
        return std::nullopt;

    reader r{frame};
    if(r.u8() != static_cast<std::uint8_t>(udp_arq_kind::ack))
        return std::nullopt;

    udp_ack ack;
    ack.cumulative = r.u16();
    r.copy_to(reinterpret_cast<std::byte *>(ack.selective.data()), udp_ack::bitmap_bytes);
    return ack;
}

// Wrap a reliable data payload behind the segment marker: [segment-marker:1][payload]. Reused
// caller buffer.
inline void encode_udp_segment_into(std::vector<std::byte> &out, std::span<const std::byte> payload)
{
    out.resize(1 + payload.size());
    writer w{out};
    w.u8(static_cast<std::uint8_t>(udp_arq_kind::segment));
    w.bytes(payload);
}

// Strip the segment marker, returning the inner payload. Fail-closed: an empty frame or a
// non-segment marker yields nullopt.
inline std::optional<std::span<const std::byte>> decode_udp_segment(std::span<const std::byte> frame)
{
    reader r{frame};
    if(r.u8() != static_cast<std::uint8_t>(udp_arq_kind::segment))
        return std::nullopt;
    return r.rest();
}

// Classify a reliable_arq inner frame by its leading marker WITHOUT decoding it. A frame too
// short or with an unknown marker yields nullopt.
inline std::optional<udp_arq_kind> peek_udp_arq_kind(std::span<const std::byte> frame)
{
    reader r{frame};
    auto marker = r.u8();
    if(!r.ok())
        return std::nullopt;
    if(marker == static_cast<std::uint8_t>(udp_arq_kind::segment))
        return udp_arq_kind::segment;
    if(marker == static_cast<std::uint8_t>(udp_arq_kind::ack))
        return udp_arq_kind::ack;
    return std::nullopt;
}

}

#endif
