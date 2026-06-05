#ifndef HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/frame.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

namespace plexus::wire {

constexpr std::size_t unidirectional_header_size = 17;
constexpr std::size_t bidirectional_header_size = 41;

struct unidirectional_header
{
    endpoint_source_type source;
    uint64_t sequence;
    uint64_t topic_hash;
};

struct bidirectional_header
{
    endpoint_source_type source;
    uint64_t sequence;
    uint64_t topic_hash;
    uint64_t type_hash_1;
    uint64_t type_hash_2;
    uint64_t correlation_id;
};

struct unidirectional_decode_result
{
    unidirectional_header header;
    std::span<const std::byte> data;
};

struct bidirectional_decode_result
{
    bidirectional_header header;
    std::span<const std::byte> data;
};

inline std::vector<std::byte> encode_unidirectional(const unidirectional_header &hdr, std::span<const std::byte> data)
{
    std::vector<std::byte> buf(unidirectional_header_size + data.size());
    auto *p = buf.data();

    detail::write_u8(p, static_cast<uint8_t>(hdr.source));
    detail::write_u64(p + 1, hdr.sequence);
    detail::write_u64(p + 9, hdr.topic_hash);

    if(!data.empty())
        std::memcpy(p + unidirectional_header_size, data.data(), data.size());

    return buf;
}

// Encode a unidirectional frame into a caller-owned buffer reused across calls.
// resize() reuses the buffer's capacity, so a steady-state loop that frames into
// the same out vector allocates nothing after the warm-up grow — the building
// block of the forwarder's no-hot-path-allocation fan-out (the allocating return
// overload above stays for one-shot callers).
inline void encode_unidirectional_into(std::vector<std::byte> &out,
                                       const unidirectional_header &hdr,
                                       std::span<const std::byte> data)
{
    out.resize(unidirectional_header_size + data.size());
    auto *p = out.data();

    detail::write_u8(p, static_cast<uint8_t>(hdr.source));
    detail::write_u64(p + 1, hdr.sequence);
    detail::write_u64(p + 9, hdr.topic_hash);

    if(!data.empty())
        std::memcpy(p + unidirectional_header_size, data.data(), data.size());
}

inline std::optional<unidirectional_decode_result> decode_unidirectional(std::span<const std::byte> payload)
{
    if(payload.size() < unidirectional_header_size)
        return std::nullopt;

    auto *p = payload.data();
    unidirectional_header hdr{
            .source     = static_cast<endpoint_source_type>(detail::read_u8(p)),
            .sequence   = detail::read_u64(p + 1),
            .topic_hash = detail::read_u64(p + 9)
    };

    return unidirectional_decode_result{
            .header = hdr,
            .data   = payload.subspan(unidirectional_header_size)
    };
}

inline std::vector<std::byte> encode_bidirectional(const bidirectional_header &hdr, std::span<const std::byte> data)
{
    std::vector<std::byte> buf(bidirectional_header_size + data.size());
    auto *p = buf.data();

    detail::write_u8(p, static_cast<uint8_t>(hdr.source));
    detail::write_u64(p + 1, hdr.sequence);
    detail::write_u64(p + 9, hdr.topic_hash);
    detail::write_u64(p + 17, hdr.type_hash_1);
    detail::write_u64(p + 25, hdr.type_hash_2);
    detail::write_u64(p + 33, hdr.correlation_id);

    if(!data.empty())
        std::memcpy(p + bidirectional_header_size, data.data(), data.size());

    return buf;
}

// Encode a bidirectional frame into a caller-owned buffer reused across calls.
// resize() reuses the buffer's capacity, so a steady-state dispatch loop that
// frames into the same out vector allocates nothing after the warm-up grow —
// the rpc encoders' zero-alloc path builds on this (the allocating return
// overload above stays for one-shot callers).
inline void encode_bidirectional_into(std::vector<std::byte> &out,
                                      const bidirectional_header &hdr,
                                      std::span<const std::byte> data)
{
    out.resize(bidirectional_header_size + data.size());
    auto *p = out.data();

    detail::write_u8(p, static_cast<uint8_t>(hdr.source));
    detail::write_u64(p + 1, hdr.sequence);
    detail::write_u64(p + 9, hdr.topic_hash);
    detail::write_u64(p + 17, hdr.type_hash_1);
    detail::write_u64(p + 25, hdr.type_hash_2);
    detail::write_u64(p + 33, hdr.correlation_id);

    if(!data.empty())
        std::memcpy(p + bidirectional_header_size, data.data(), data.size());
}

inline std::optional<bidirectional_decode_result> decode_bidirectional(std::span<const std::byte> payload)
{
    if(payload.size() < bidirectional_header_size)
        return std::nullopt;

    auto *p = payload.data();
    bidirectional_header hdr{
            .source         = static_cast<endpoint_source_type>(detail::read_u8(p)),
            .sequence       = detail::read_u64(p + 1),
            .topic_hash     = detail::read_u64(p + 9),
            .type_hash_1    = detail::read_u64(p + 17),
            .type_hash_2    = detail::read_u64(p + 25),
            .correlation_id = detail::read_u64(p + 33)
    };

    return bidirectional_decode_result{
            .header = hdr,
            .data   = payload.subspan(bidirectional_header_size)
    };
}

}

#endif
