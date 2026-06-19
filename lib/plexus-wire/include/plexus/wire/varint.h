#ifndef HPP_GUARD_PLEXUS_WIRE_VARINT_H
#define HPP_GUARD_PLEXUS_WIRE_VARINT_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// LEB128 variable-length codec for an unsigned 64-bit counter.
//
// Trust contract — WIRE side (untrusted input):
//   read_varint rides the same untrusted-input surface as read_length_prefixed:
//   the bytes may have arrived from a peer whose intent is unverified, so a
//   fully-formed frame can carry any continuation-byte sequence. The decoder is
//   the bounds gate for the gid endpoint-counter, so it must reject every
//   malformed shape rather than over-read or wrap.
//
// The decoder enforces, in order:
//   (a) `consumed <= data.size()`               — caller-supplied offset
//   (b) the continuation-byte loop is capped at `k_max_varint_bytes` (10, the
//       maximum LEB128 length of a u64); an 11th continuation byte is rejected
//   (c) the 10th byte may carry at most a single payload bit (bit 63); any
//       higher bit would shift past u64, so the accumulate is overflow-guarded
//       BEFORE it runs
//   (d) a continuation bit still set on the 10th byte (no terminator within the
//       cap) is rejected
//
// Returns:
//   - On success: the decoded counter. `consumed` is advanced by exactly the
//     number of bytes the encoding occupied.
//   - On any violation (truncation, over-length, overflow): std::nullopt, and
//     `consumed` is NOT advanced — the caller can discriminate a malformed
//     varint from a partial-buffer underrun.
constexpr std::size_t k_max_varint_bytes = 10;

namespace detail {

// Accumulate one LEB128 byte at index i into value. Returns true when the terminator (no
// continuation bit) was seen. On the 10th (last permitted) byte only bit 63 may be set — any
// higher payload bit would overflow u64 — so the overflow guard runs BEFORE the accumulate.
[[nodiscard]] inline bool varint_step(std::uint8_t byte, std::size_t i, std::uint64_t &value,
                                      bool &overflow) noexcept
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

[[nodiscard]] inline std::optional<std::uint64_t> read_varint(std::span<const std::byte> data,
                                                              std::size_t &consumed) noexcept
{
    if(consumed > data.size())
        return std::nullopt;

    std::uint64_t value  = 0;
    std::size_t   offset = consumed;
    for(std::size_t i = 0; i < k_max_varint_bytes; ++i)
    {
        if(offset >= data.size())
            return std::nullopt; // truncated: ran out of bytes before a terminator
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
    return std::nullopt; // continuation bit still set on the 10th byte: over-long
}

// The byte length the LEB128 encoding of `value` will occupy (1..10 for a u64),
// so a one-pass writer can size a region before emitting the counter inline.
[[nodiscard]] inline std::size_t varint_size(std::uint64_t value) noexcept
{
    std::size_t bytes = 1;
    while((value >>= 7u) != 0)
        ++bytes;
    return bytes;
}

// Append the LEB128 encoding of `value` to `out`. Emits one byte per 7 value
// bits (1..10 bytes for a u64), mirroring the zero-clear append discipline of the
// codec's `_into` writers: the caller's existing bytes are preserved.
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
