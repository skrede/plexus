#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_FORWARDED_FRAME_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_FORWARDED_FRAME_CODEC_H

#include "plexus/wire/reader.h"
#include "plexus/wire/writer.h"
#include "plexus/wire/forwarded_frame.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

namespace detail {

// The inner byte count actually put on the wire: clamped to the decoder's ceiling so the u32 length
// prefix can never wrap. encode and encoded_size share it so the buffer and the count agree.
inline std::size_t effective_inner_size(const forwarded_frame &ff) noexcept
{
    return ff.inner.size() < k_forwarded_inner_max ? ff.inner.size() : k_forwarded_inner_max;
}

inline std::size_t forwarded_frame_encoded_size(const forwarded_frame &ff) noexcept
{
    return forwarded_frame_preamble_size + sizeof(std::uint32_t) + effective_inner_size(ff);
}

}

// Encode into a caller-pre-sized region (a pool slot on the relay splice) and return the byte count,
// so the splice frames the envelope with no intermediate buffer. region must hold at least
// forwarded_frame_encoded_size(ff) bytes.
inline std::size_t encode_forwarded_frame_into(std::span<std::byte> region, const forwarded_frame &ff) noexcept
{
    writer w{region};
    w.bytes(std::span<const std::byte>{ff.origin.data(), ff.origin.size()});
    w.bytes(std::span<const std::byte>{ff.destination.data(), ff.destination.size()});
    w.u8(ff.hop);
    w.u16(ff.seq);
    w.u8(ff.flags);
    const std::size_t n = detail::effective_inner_size(ff);
    w.u32(static_cast<std::uint32_t>(n));
    w.bytes(std::span<const std::byte>{ff.inner.data(), n});
    return w.offset();
}

inline std::vector<std::byte> encode_forwarded_frame(const forwarded_frame &ff)
{
    std::vector<std::byte> buf(detail::forwarded_frame_encoded_size(ff));
    encode_forwarded_frame_into(buf, ff);
    return buf;
}

// Decode off an untrusted session frame. The fixed preamble then the u32-length-prefixed inner region
// are read against the latching reader; the inner length is capped against both the payload remainder
// (by length_prefixed) and the ceiling BEFORE any copy, and ok() is tested once: a truncated frame or
// an inner prefix past the remainder yields nullopt with no partial struct and no over-read.
inline std::optional<forwarded_frame> decode_forwarded_frame(std::span<const std::byte> payload)
{
    reader r{payload};
    forwarded_frame ff{};
    r.copy_to(ff.origin.data(), ff.origin.size());
    r.copy_to(ff.destination.data(), ff.destination.size());
    ff.hop   = r.u8();
    ff.seq   = r.u16();
    ff.flags = r.u8();

    const auto inner = r.length_prefixed<std::uint32_t>();
    if(inner.size() > detail::k_forwarded_inner_max)
        return std::nullopt;
    if(!r.ok())
        return std::nullopt;
    ff.inner.assign(inner.begin(), inner.end());
    return ff;
}

}

#endif
