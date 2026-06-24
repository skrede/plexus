#ifndef HPP_GUARD_PLEXUS_WIRE_VARINT_H
#define HPP_GUARD_PLEXUS_WIRE_VARINT_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// LEB128 codec for an unsigned 64-bit counter, decoding untrusted input. The decoder
// enforces, in order:
//   (a) `consumed <= data.size()`               — caller-supplied offset
//   (b) the continuation loop is capped at `k_max_varint_bytes` (10, the maximum LEB128
//       length of a u64); an 11th continuation byte is rejected
//   (c) the 10th byte may carry at most a single payload bit (bit 63); a higher bit would
//       shift past u64, so the accumulate is overflow-guarded BEFORE it runs
//   (d) a continuation bit still set on the 10th byte is rejected
// On any violation `consumed` is NOT advanced, so a malformed varint is discriminable from
// a partial-buffer underrun.
constexpr std::size_t k_max_varint_bytes = 10;

namespace detail {

// Returns true when the terminator (no continuation bit) is seen. On the 10th byte only bit
// 63 may be set, so the overflow guard runs BEFORE the accumulate.
inline bool varint_step(std::uint8_t byte, std::size_t i, std::uint64_t &value, bool &overflow) noexcept
{
    const std::uint64_t payload = byte & 0x7Fu;
    if(i == k_max_varint_bytes - 1 && payload > 0x01u)
    {
        overflow = true;
        return true;
    }
    value |= payload << (7u * i);
    return (byte & 0x80u) == 0;
}

}

inline std::optional<std::uint64_t> read_varint(std::span<const std::byte> data, std::size_t &consumed) noexcept
{
    if(consumed > data.size())
        return std::nullopt;

    std::uint64_t value = 0;
    std::size_t offset  = consumed;
    for(std::size_t i = 0; i < k_max_varint_bytes; ++i)
    {
        if(offset >= data.size())
            return std::nullopt;
        const auto byte = static_cast<std::uint8_t>(data[offset]);
        ++offset;

        bool overflow = false;
        if(detail::varint_step(byte, i, value, overflow))
        {
            if(overflow)
                return std::nullopt;
            consumed = offset;
            return value;
        }
    }
    return std::nullopt;
}

// The byte length the LEB128 encoding of `value` occupies (1..10 for a u64), so a one-pass
// writer can size a region before emitting the counter inline.
inline std::size_t varint_size(std::uint64_t value) noexcept
{
    std::size_t bytes = 1;
    while((value >>= 7u) != 0)
        ++bytes;
    return bytes;
}

// Append the LEB128 encoding of `value` to `out`, preserving the caller's existing bytes.
inline void write_varint(std::vector<std::byte> &out, std::uint64_t value)
{
    do
    {
        auto byte = static_cast<std::uint8_t>(value & 0x7Fu);
        value >>= 7u;
        if(value != 0)
            byte |= 0x80u;
        out.push_back(static_cast<std::byte>(byte));
    } while(value != 0);
}

}

#endif
