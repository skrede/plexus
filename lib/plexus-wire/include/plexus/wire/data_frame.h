#ifndef HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_DATA_FRAME_H

#include "plexus/wire/frame.h"
#include "plexus/wire/cursor.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

constexpr std::size_t unidirectional_header_size = 17;
constexpr std::size_t bidirectional_header_size  = 41;

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
    // Structural reservation words, kept ZEROED on the data path (they carry no type-matching
    // or correlation authority). They hold the byte layout (bidirectional_header_size stays 41)
    // so activating them later is a wire-size-stable change.
    uint64_t type_hash_1;
    uint64_t type_hash_2;
    uint64_t correlation_id;
};

struct unidirectional_decode_result
{
    unidirectional_header header;
    // Present iff decoded with has_source_identity (the frame's gid flag was set). The receiver
    // pairs it with the session peer's node_id to reconstruct the publisher_gid.
    std::optional<std::uint64_t> endpoint_counter;
    std::span<const std::byte> data;
};

struct bidirectional_decode_result
{
    bidirectional_header header;
    std::span<const std::byte> data;
};

}

#include "plexus/wire/detail/data_frame_codec.h"

#endif
