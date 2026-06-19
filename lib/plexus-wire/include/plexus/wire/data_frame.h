#ifndef HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/frame_codec.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace plexus::wire {

constexpr std::size_t unidirectional_header_size = 17;
constexpr std::size_t bidirectional_header_size  = 41;

struct unidirectional_header
{
    endpoint_source_type source;
    uint64_t             sequence;
    uint64_t             topic_hash;
};

struct bidirectional_header
{
    endpoint_source_type source;
    uint64_t             sequence;
    uint64_t             topic_hash;
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
    std::span<const std::byte>   data;
};

struct bidirectional_decode_result
{
    bidirectional_header       header;
    std::span<const std::byte> data;
};

}

// The encode/decode codecs over the shapes above are relocated to detail/data_frame_codec.h; the
// include keeps every wire::encode_*/decode_* data-frame call site resolving unchanged.
#include "plexus/wire/detail/data_frame_codec.h"

#endif
