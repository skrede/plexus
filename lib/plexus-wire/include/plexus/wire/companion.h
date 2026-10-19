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

// The payload of companion_ready: the topic and the ring slot of the subscriber's reader, which the publisher places at
// the first message it routes to the subscriber over the lane.
struct companion_ready
{
    std::uint64_t topic_hash;
    std::uint32_t reader;
};

constexpr std::size_t companion_ready_size = 12;

inline std::array<std::byte, companion_ready_size> encode_companion_ready(const companion_ready &ready)
{
    std::array<std::byte, companion_ready_size> buf{};
    wire::detail::write_u64(buf.data(), ready.topic_hash);
    wire::detail::write_u32(buf.data() + 8, ready.reader);
    return buf;
}

inline std::optional<companion_ready> decode_companion_ready(std::span<const std::byte> payload)
{
    if(payload.size() != companion_ready_size)
        return std::nullopt;
    return companion_ready{wire::detail::read_u64(payload.data()), wire::detail::read_u32(payload.data() + 8)};
}

// The payload of companion_switch: the publication sequence of the first message the publisher routes to the subscriber
// over the topic's shared-memory lane.
struct companion_switch
{
    std::uint64_t topic_hash;
    std::uint64_t first_sequence;
};

constexpr std::size_t companion_switch_size = 16;

inline std::array<std::byte, companion_switch_size> encode_companion_switch(const companion_switch &marker)
{
    std::array<std::byte, companion_switch_size> buf{};
    wire::detail::write_u64(buf.data(), marker.topic_hash);
    wire::detail::write_u64(buf.data() + 8, marker.first_sequence);
    return buf;
}

inline std::optional<companion_switch> decode_companion_switch(std::span<const std::byte> payload)
{
    if(payload.size() != companion_switch_size)
        return std::nullopt;
    return companion_switch{wire::detail::read_u64(payload.data()), wire::detail::read_u64(payload.data() + 8)};
}

}

#endif
