#ifndef HPP_GUARD_PLEXUS_WIRE_COMPANION_H
#define HPP_GUARD_PLEXUS_WIRE_COMPANION_H

#include "plexus/wire/byte_order.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The payload of companion_ready and companion_withdrawn: the topic whose shared-memory receive companion the
// subscriber reports on.
struct companion_signal
{
    std::uint64_t topic_hash;
};

constexpr std::size_t companion_signal_size = 8;

inline std::array<std::byte, companion_signal_size> encode_companion(const companion_signal &signal)
{
    std::array<std::byte, companion_signal_size> buf{};
    wire::detail::write_u64(buf.data(), signal.topic_hash);
    return buf;
}

// The payload is fixed-size, so any other length is refused.
inline std::optional<companion_signal> decode_companion(std::span<const std::byte> payload)
{
    if(payload.size() != companion_signal_size)
        return std::nullopt;
    return companion_signal{wire::detail::read_u64(payload.data())};
}

}

#endif
