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

// Bounds-only length-prefix decoder over untrusted bytes — the primary bounds gate. It
// enforces, in order:
//   (a) `consumed <= data.size()`                     — caller-supplied offset
//   (b) `data.size() - consumed >= sizeof(UIntT)`     — prefix fits
//   (c) `length * element_size` does NOT overflow     — pre-multiply guard
//   (d) `length * element_size <= remaining bytes`    — payload fits
// (d) is the pre-subtracted-RHS idiom: subtract sizeof(UIntT) first (cannot wrap, b holds),
// then compare. On any violation `consumed` is NOT advanced, so a malformed prefix is
// discriminable from a partial-buffer underrun. Per-callsite policy caps are NOT enforced
// here; callers apply those after consuming the returned span.
//
// element_size default 1 (length in bytes). For T-typed arrays whose prefix counts elements,
// pass sizeof(T); element_size = 0 short-circuits to the length-only, no-payload check.
namespace detail {

// Width-dispatched big-endian prefix read at `at`, resolved at compile time per UIntT.
template<typename UIntT>
inline UIntT read_length_value(const std::byte *at) noexcept
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
std::optional<std::span<const std::byte>> read_length_prefixed(std::span<const std::byte> data, std::size_t &consumed, std::size_t element_size = 1) noexcept
{
    static_assert(std::is_unsigned_v<UIntT>, "read_length_prefixed: UIntT must be unsigned");

    if(consumed > data.size())
        return std::nullopt;
    const auto remaining_after_consumed = data.size() - consumed;
    if(remaining_after_consumed < sizeof(UIntT))
        return std::nullopt;

    const UIntT length   = detail::read_length_value<UIntT>(data.data() + consumed);
    const auto length_sz = static_cast<std::size_t>(length);
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
