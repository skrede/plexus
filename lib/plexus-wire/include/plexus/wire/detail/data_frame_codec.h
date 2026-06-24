#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_DATA_FRAME_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_DATA_FRAME_CODEC_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// Encode a unidirectional frame into a caller-owned buffer reused across calls (resize() reuses
// capacity, so a steady-state loop allocates nothing after warm-up). endpoint_counter is the
// flag-gated source-identity region: when present a varint counter nests between the fixed 17B
// header and the data, and the caller sets frame_header.flags |= k_flag_source_identity.
inline void encode_unidirectional_into(std::vector<std::byte> &out, const unidirectional_header &hdr, std::span<const std::byte> data,
                                       std::optional<std::uint64_t> endpoint_counter = std::nullopt)
{
    out.resize(unidirectional_header_size);
    writer w{out};
    w.u8(static_cast<uint8_t>(hdr.source));
    w.u64(hdr.sequence);
    w.u64(hdr.topic_hash);
    if(endpoint_counter)
        write_varint(out, *endpoint_counter);
    out.insert(out.end(), data.begin(), data.end());
}

inline std::vector<std::byte> encode_unidirectional(const unidirectional_header &hdr, std::span<const std::byte> data, std::optional<std::uint64_t> endpoint_counter = std::nullopt)
{
    std::vector<std::byte> out;
    encode_unidirectional_into(out, hdr, data, endpoint_counter);
    return out;
}

// One-pass combined framing: write [frame_header][unidirectional_header][counter?][payload] into
// ONE reused buffer, inserting the payload exactly once. Byte-identical to
// encode_unidirectional_into then encode_frame_into, but without the intermediate inner buffer and
// its second payload copy.
inline void encode_unidirectional_frame_into(std::vector<std::byte> &out, const frame_header &fhdr, const unidirectional_header &uhdr, std::span<const std::byte> payload,
                                             std::optional<std::uint64_t> endpoint_counter = std::nullopt)
{
    const std::size_t counter_len = endpoint_counter ? varint_size(*endpoint_counter) : 0;
    const std::size_t payload_len = unidirectional_header_size + counter_len + payload.size();

    auto adjusted           = fhdr;
    adjusted.payload_len    = payload_len;
    const auto header_bytes = encode_header(adjusted);

    out.resize(header_size + payload_len);
    writer w{out};
    w.bytes(header_bytes);
    w.u8(static_cast<uint8_t>(uhdr.source));
    w.u64(uhdr.sequence);
    w.u64(uhdr.topic_hash);
    if(endpoint_counter)
        w.varint(*endpoint_counter);
    w.bytes(payload);
}

// Decode a unidirectional payload. has_source_identity MUST mirror the frame's gid flag: when set,
// a varint endpoint counter follows the fixed header and is decoded through the bounds-safe
// read_varint (a truncated/over-long region returns nullopt → the whole decode fails). When clear,
// the data begins immediately after the 17B header (the v3-no-flag layout).
inline std::optional<unidirectional_decode_result> decode_unidirectional(std::span<const std::byte> payload, bool has_source_identity = false)
{
    if(payload.size() < unidirectional_header_size)
        return std::nullopt;

    reader                r{payload};
    unidirectional_header hdr{.source = static_cast<endpoint_source_type>(r.u8()), .sequence = r.u64(), .topic_hash = r.u64()};

    if(!has_source_identity)
        return unidirectional_decode_result{.header = hdr, .endpoint_counter = std::nullopt, .data = r.rest()};

    auto counter = r.varint();
    if(!counter)
        return std::nullopt;
    return unidirectional_decode_result{.header = hdr, .endpoint_counter = counter, .data = r.rest()};
}

inline void encode_bidirectional_into(std::vector<std::byte> &out, const bidirectional_header &hdr, std::span<const std::byte> data)
{
    out.resize(bidirectional_header_size + data.size());
    writer w{out};
    w.u8(static_cast<uint8_t>(hdr.source));
    w.u64(hdr.sequence);
    w.u64(hdr.topic_hash);
    w.u64(hdr.type_hash_1);
    w.u64(hdr.type_hash_2);
    w.u64(hdr.correlation_id);
    w.bytes(data);
}

inline std::vector<std::byte> encode_bidirectional(const bidirectional_header &hdr, std::span<const std::byte> data)
{
    std::vector<std::byte> buf;
    encode_bidirectional_into(buf, hdr, data);
    return buf;
}

inline std::optional<bidirectional_decode_result> decode_bidirectional(std::span<const std::byte> payload)
{
    if(payload.size() < bidirectional_header_size)
        return std::nullopt;

    reader               r{payload};
    bidirectional_header hdr{
            .source = static_cast<endpoint_source_type>(r.u8()), .sequence = r.u64(), .topic_hash = r.u64(), .type_hash_1 = r.u64(), .type_hash_2 = r.u64(), .correlation_id = r.u64()};

    return bidirectional_decode_result{.header = hdr, .data = r.rest()};
}

}

#endif
