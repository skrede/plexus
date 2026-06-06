#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_ACK_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_ACK_H

#include "plexus/wire/byte_order.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

namespace plexus::wire {

// The reliable-ARQ inner control discriminator. A reliable_arq (kind=1) datagram
// self-identifies by a LEADING control byte so ONE inbound demux path handles both a
// data segment and an ack/nack control frame. The marker values start at 2 — disjoint
// from the handshake control bytes (request=0, response=1, which are recognized by a
// stricter exactly-one-byte handshake decode) so the three inner-frame families never
// alias: a handshake frame is exactly [0|1]; a data segment is [segment][payload..];
// an ack frame is [ack][cumulative:2][hole-bitmap..]. The seq the segment carries
// lives in the OUTER udp_envelope, not here.
enum class udp_arq_kind : std::uint8_t
{
    segment = 2,
    ack = 3,
};

// A cumulative + selective acknowledgment. `cumulative` is the highest in-order seq
// the receiver has delivered (expected - 1, the left edge the sender's window may
// slide to); the selective bitmap names the buffered holes ABOVE the cumulative edge
// the sender must selectively retransmit — bit i set means (cumulative + 2 + i) was
// received out of order and need not be resent. A bounded fixed bitmap (no hot-path
// alloc): a 256-bit (32-byte) window of holes, ample for the default send window.
struct udp_ack
{
    // bitmap_bits names the holes ABOVE the cumulative edge a single ack can describe.
    // The send window may legitimately EXCEED this count: holes beyond offset bitmap_bits
    // are not nameable in the selective bitmap and fall back to per-segment RTO-driven
    // retransmit (idempotent at the receiver). The reliable ARQ binds its window's upper
    // sweep bound to this constant via a static_assert (see udp_reliable_arq.h) so a future
    // window sweep cannot silently pick a width whose excess is undescribable by surprise.
    static constexpr std::size_t bitmap_bytes = 32;          // 256 holes named per ack
    static constexpr std::size_t bitmap_bits = bitmap_bytes * 8;

    std::uint16_t cumulative{0};
    std::array<std::uint8_t, bitmap_bytes> selective{};      // bit i -> (cumulative+2+i) received

    [[nodiscard]] bool hole_received(std::size_t i) const noexcept
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

// Encode an ack into a caller-owned buffer reused across sends (no per-ack alloc
// after warm-up). The leading marker self-identifies the frame to the inbound demux.
inline void encode_udp_ack_into(std::vector<std::byte> &out, const udp_ack &ack)
{
    out.resize(udp_ack_frame_size);
    auto *p = out.data();

    detail::write_u8(p, static_cast<std::uint8_t>(udp_arq_kind::ack));
    detail::write_u16(p + 1, ack.cumulative);
    std::memcpy(p + 3, ack.selective.data(), udp_ack::bitmap_bytes);
}

// Decode an untrusted ack control frame. Fail-closed: a frame that is not exactly the
// fixed ack size, or whose leading marker is not the ack discriminator, decodes to
// nullopt (the caller drops it). The size check precedes every read.
inline std::optional<udp_ack> decode_udp_ack(std::span<const std::byte> frame)
{
    if(frame.size() != udp_ack_frame_size)
        return std::nullopt;

    auto *p = frame.data();
    if(detail::read_u8(p) != static_cast<std::uint8_t>(udp_arq_kind::ack))
        return std::nullopt;

    udp_ack ack;
    ack.cumulative = detail::read_u16(p + 1);
    std::memcpy(ack.selective.data(), p + 3, udp_ack::bitmap_bytes);
    return ack;
}

// Wrap a reliable data payload behind the segment marker: [segment-marker:1][payload].
// The marker keeps a data segment distinguishable from an ack on the one demux path;
// the segment's seq rides the outer envelope. Reused buffer, no per-send alloc.
inline void encode_udp_segment_into(std::vector<std::byte> &out, std::span<const std::byte> payload)
{
    out.resize(1 + payload.size());
    detail::write_u8(out.data(), static_cast<std::uint8_t>(udp_arq_kind::segment));
    if(!payload.empty())
        std::memcpy(out.data() + 1, payload.data(), payload.size());
}

// Strip the segment marker, returning the inner payload. Fail-closed: an empty frame
// or a non-segment marker yields nullopt (the caller drops / dispatches elsewhere).
inline std::optional<std::span<const std::byte>> decode_udp_segment(std::span<const std::byte> frame)
{
    if(frame.empty() || detail::read_u8(frame.data()) != static_cast<std::uint8_t>(udp_arq_kind::segment))
        return std::nullopt;
    return frame.subspan(1);
}

// Classify a reliable_arq inner frame by its leading marker WITHOUT decoding it. Used
// by the channel's single demux path to fan a kind=1 inner frame to on_segment vs
// on_ack. A frame too short or with an unknown marker yields nullopt (dropped).
inline std::optional<udp_arq_kind> peek_udp_arq_kind(std::span<const std::byte> frame)
{
    if(frame.empty())
        return std::nullopt;
    auto marker = detail::read_u8(frame.data());
    if(marker == static_cast<std::uint8_t>(udp_arq_kind::segment))
        return udp_arq_kind::segment;
    if(marker == static_cast<std::uint8_t>(udp_arq_kind::ack))
        return udp_arq_kind::ack;
    return std::nullopt;
}

}

#endif
