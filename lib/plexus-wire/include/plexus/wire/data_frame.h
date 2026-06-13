#ifndef HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/varint.h"

#include <cstddef>
#include <cstdint>
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
    // A pair of structural reservation words, kept ZEROED on the data path. They
    // carry NO type-matching authority: type matching is settled at subscribe-time
    // discovery (subscribe_request.type_hash), not per data frame. They also carry
    // NO correlation role — req/res is matched solely by correlation_id (the
    // pending-table key). They remain in the header only to hold the byte layout
    // (bidirectional_header_size stays 41) so activating them is an append-free,
    // wire-size-stable change if a future feature ever needs them.
    uint64_t type_hash_1;
    uint64_t type_hash_2;
    uint64_t correlation_id;
};

struct unidirectional_decode_result
{
    unidirectional_header header;
    // The decoded source-identity endpoint counter, present iff the caller decoded
    // with has_source_identity (the frame's gid flag was set). The receiver pairs it
    // with the session peer's node_id to reconstruct the publisher_gid; absent leaves
    // message_info.source_identity unset.
    std::optional<std::uint64_t> endpoint_counter;
    std::span<const std::byte> data;
};

struct bidirectional_decode_result
{
    bidirectional_header header;
    std::span<const std::byte> data;
};

// Encode a unidirectional frame into a caller-owned buffer reused across calls.
// resize() reuses the buffer's capacity, so a steady-state loop that frames into
// the same out vector allocates nothing after the warm-up grow — the building
// block of the forwarder's no-hot-path-allocation fan-out.
//
// endpoint_counter is the flag-gated source-identity region: when present, a varint
// endpoint counter nests between the fixed 17B header and the data (inside
// payload_len), and the caller sets frame_header.flags |= k_flag_source_identity.
// Absent → byte-identical to a v3-no-flag frame. write_varint/insert reuse the
// buffer's existing capacity, so the no-alloc property holds in both shapes.
inline void encode_unidirectional_into(std::vector<std::byte> &out,
                                       const unidirectional_header &hdr,
                                       std::span<const std::byte> data,
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

// One-shot allocating overload for callers that do not reuse a buffer; delegates to
// the reusing _into form so the layout (incl. the flag-gated counter) lives in ONE place.
inline std::vector<std::byte> encode_unidirectional(const unidirectional_header &hdr,
                                                    std::span<const std::byte> data,
                                                    std::optional<std::uint64_t> endpoint_counter = std::nullopt)
{
    std::vector<std::byte> out;
    encode_unidirectional_into(out, hdr, data, endpoint_counter);
    return out;
}

// Decode a unidirectional payload. has_source_identity MUST mirror the frame's gid
// flag (k_flag_source_identity in frame_header.flags): when set, a varint endpoint
// counter follows the fixed header and is decoded through the bounds-safe read_varint
// (a truncated or over-long region returns nullopt → the whole decode fails, the
// warn-and-drop path). When clear, the data begins immediately after the 17B header
// (the v3-no-flag layout). Both the gid-bearing and the bytes-only receive tails
// pass the flag so the data span is correct regardless of which callback is set.
inline std::optional<unidirectional_decode_result>
decode_unidirectional(std::span<const std::byte> payload, bool has_source_identity = false)
{
    if(payload.size() < unidirectional_header_size)
        return std::nullopt;

    reader r{payload};
    unidirectional_header hdr{
            .source     = static_cast<endpoint_source_type>(r.u8()),
            .sequence   = r.u64(),
            .topic_hash = r.u64()
    };

    if(!has_source_identity)
        return unidirectional_decode_result{
                .header           = hdr,
                .endpoint_counter = std::nullopt,
                .data             = r.rest()
        };

    auto counter = r.varint();
    if(!counter)
        return std::nullopt;
    return unidirectional_decode_result{
            .header           = hdr,
            .endpoint_counter = counter,
            .data             = r.rest()
    };
}

inline std::vector<std::byte> encode_bidirectional(const bidirectional_header &hdr, std::span<const std::byte> data)
{
    std::vector<std::byte> buf(bidirectional_header_size + data.size());
    writer w{buf};

    w.u8(static_cast<uint8_t>(hdr.source));
    w.u64(hdr.sequence);
    w.u64(hdr.topic_hash);
    w.u64(hdr.type_hash_1);
    w.u64(hdr.type_hash_2);
    w.u64(hdr.correlation_id);
    w.bytes(data);

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
    writer w{out};

    w.u8(static_cast<uint8_t>(hdr.source));
    w.u64(hdr.sequence);
    w.u64(hdr.topic_hash);
    w.u64(hdr.type_hash_1);
    w.u64(hdr.type_hash_2);
    w.u64(hdr.correlation_id);
    w.bytes(data);
}

inline std::optional<bidirectional_decode_result> decode_bidirectional(std::span<const std::byte> payload)
{
    if(payload.size() < bidirectional_header_size)
        return std::nullopt;

    reader r{payload};
    bidirectional_header hdr{
            .source         = static_cast<endpoint_source_type>(r.u8()),
            .sequence       = r.u64(),
            .topic_hash     = r.u64(),
            .type_hash_1    = r.u64(),
            .type_hash_2    = r.u64(),
            .correlation_id = r.u64()
    };

    return bidirectional_decode_result{
            .header = hdr,
            .data   = r.rest()
    };
}

}

#endif
