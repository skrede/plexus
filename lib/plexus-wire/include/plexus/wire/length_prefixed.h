#ifndef HPP_GUARD_PLEXUS_WIRE_LENGTH_PREFIXED_H
#define HPP_GUARD_PLEXUS_WIRE_LENGTH_PREFIXED_H

#include "plexus/wire/byte_order.h"

#include <span>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

namespace plexus::wire {

// Bounds-only length-prefix decoder over std::span<const std::byte>.
//
// Trust contract — WIRE side (untrusted input):
//   Caller passes raw bytes that may have arrived from a peer whose intent
//   is unverified. The upstream `frame_reassembler::feed` has applied a
//   per-frame buffered-bytes cap, but the decoded payload here is still
//   attacker-shaped: a fully-formed frame can carry any UIntT length prefix
//   value within [0, max(UIntT)]. The helper is the primary bounds gate.
//
// The decoder enforces, in order:
//   (a) `consumed <= data.size()`                     — caller-supplied offset
//   (b) `data.size() - consumed >= sizeof(UIntT)`     — prefix fits
//   (c) `length * element_size` does NOT overflow     — pre-multiply guard
//   (d) `length * element_size <= remaining bytes`    — payload fits
//
// (d) is expressed as `payload_bytes > (remaining - sizeof(UIntT))`,
// the pre-subtracted-RHS idiom: subtract first (cannot wrap because we
// just established b), then compare. Matches the project-wide no-wrap-
// by-construction convention used by the codec-side parsers.
//
// Returns:
//   - On success: a span over the payload bytes, exclusive of the prefix.
//                 `consumed` is advanced by sizeof(UIntT) + payload_bytes.
//   - On any violation: std::nullopt. `consumed` is NOT advanced, so the
//                       caller can either retry under a different contract
//                       or discriminate from a partial-buffer underrun.
//
// NOT enforced:
//   - Per-callsite policy caps. Callers apply those inline after consuming
//     the returned payload span.
//   - Per-element validation (the helper does not know the element type).
//
// element_size:
//   - Default = 1 (length is in bytes).
//   - For T-typed arrays where the prefix counts elements, pass sizeof(T)
//     and the helper applies the overflow / bounds checks against
//     length * element_size. element_size = 0 short-circuits to the
//     length-only check and is treated as a no-payload variant.
namespace detail {

// Width-dispatched big-endian prefix read at `at` (the byte_order readers are little-only on the
// wire side). The width is resolved at compile time per UIntT.
template<typename UIntT>
[[nodiscard]] inline UIntT read_length_value(const std::byte *at) noexcept
{
    if constexpr(sizeof(UIntT) == 1)
        return read_u8(at);
    else if constexpr(sizeof(UIntT) == 2)
        return read_u16(at);
    else if constexpr(sizeof(UIntT) == 4)
        return read_u32(at);
    else if constexpr(sizeof(UIntT) == 8)
        return read_u64(at);
    else
        static_assert(sizeof(UIntT) == 0, "read_length_prefixed: unsupported UIntT width");
}

}

template<typename UIntT>
[[nodiscard]] std::optional<std::span<const std::byte>>
read_length_prefixed(std::span<const std::byte> data, std::size_t &consumed,
                     std::size_t element_size = 1) noexcept
{
    static_assert(std::is_unsigned_v<UIntT>, "read_length_prefixed: UIntT must be unsigned");

    if(consumed > data.size())
        return std::nullopt;
    const auto remaining_after_consumed = data.size() - consumed;
    if(remaining_after_consumed < sizeof(UIntT))
        return std::nullopt;

    const UIntT length    = detail::read_length_value<UIntT>(data.data() + consumed);
    const auto  length_sz = static_cast<std::size_t>(length);
    if(element_size != 0 && length_sz > std::numeric_limits<std::size_t>::max() / element_size)
        return std::nullopt;
    const auto payload_bytes = length_sz * element_size;

    const auto after_prefix = remaining_after_consumed - sizeof(UIntT);
    if(payload_bytes > after_prefix)
        return std::nullopt;

    const auto payload_offset = consumed + sizeof(UIntT);
    consumed                  = payload_offset + payload_bytes;
    return std::span<const std::byte>{data.data() + payload_offset, payload_bytes};
}

}

#endif
