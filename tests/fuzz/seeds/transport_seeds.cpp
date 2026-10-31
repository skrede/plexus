#include "seed_sink.h"

#include "fuzz/fuzz_region.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/node_id.h"

#include <span>
#include <array>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <string_view>
#include <initializer_list>

namespace {

using namespace plexus;

constexpr node_id k_publisher{std::byte{0x5a}, std::byte{0xa5}};
constexpr std::uint64_t k_cells = 4;
constexpr std::uint64_t k_slot  = 16;

// The ring driver's steps: register and publish a reader, commit 16 bytes, consume, drain, commit a short claim,
// repair, unregister.
constexpr std::array<std::uint8_t, 19> k_ring_script{8, 0, 1, 1, 5, 1, 16, 2, 0, 7, 1, 1, 1, 8, 1, 8, 8, 3, 0};

// A created ring whose enqueue position a peer moved half the sequence space past every cell's sequence, then a
// best-effort claim.
constexpr std::array<std::uint8_t, 10> k_far_enqueue{0x08, 0x3d, 0x0d, 0x88, 0x01, 0x07, 0x00, 0x80, 0x7a, 0x13};

// Grammar: an even mode byte, the cell exponent, slot size minus one, consumer capacity minus one, the tamper count and
// edits, then the driver's steps.
seed ring_created(std::uint8_t mode, std::initializer_list<std::uint8_t> geometry)
{
    seed out = prefixed({mode}, {});
    append(out, geometry);
    append(out, std::as_bytes(std::span{k_ring_script}));
    return out;
}

// Grammar: an odd mode byte, the image length (little-endian u16), a control image a creator wrote, the driver's steps.
seed ring_image(std::uint8_t mode)
{
    fuzz_region control{shm::control_region_bytes(k_cells)};
    fuzz_region slab{shm::slab_region_bytes(k_cells, k_slot)};
    shm::broadcast_ring ring;
    if(shm::broadcast_ring::create(control.bytes(), slab.bytes(), k_cells, k_slot, k_publisher, ring, 2) != shm::loan_status::ok)
        std::abort();
    const std::size_t size = control.bytes().size();
    seed out               = prefixed({mode, static_cast<std::uint8_t>(size), static_cast<std::uint8_t>(size >> 8)}, control.bytes());
    append(out, std::as_bytes(std::span{k_ring_script}));
    return out;
}

seed framed(wire::msg_type type, std::span<const std::byte> payload)
{
    return wire::encode_frame({.type = type, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0}, payload);
}

seed fragment(std::uint16_t msg_id, std::uint32_t index, std::uint32_t count, std::string_view slice)
{
    seed out;
    wire::encode_udp_fragment_payload_into(out, msg_id, index, count, text(slice));
    return out;
}

// Grammar: the start offset, filler up to it, then the encoding.
seed varint_at(std::uint8_t offset, std::uint64_t value)
{
    seed out = prefixed({offset}, {});
    out.resize(std::max<std::size_t>(offset, 1), std::byte{0x7f});
    wire::write_varint(out, value);
    return out;
}

}

seed_set ring_attach_seeds()
{
    return {
            ring_created(0, {2, 15, 1, 0}),
            ring_created(0, {2, 15, 1, 1, 0x08, 0x00, 0xff}),
            ring_created(2, {0, 0, 0, 0}),
            ring_created(4, {1, 7, 0, 0}),
            ring_image(1),
            ring_image(3),
            ring_image(5),
            prefixed({}, std::as_bytes(std::span{k_far_enqueue})),
    };
}

seed_set frame_reassembler_seeds()
{
    seed pair = framed(wire::msg_type::heartbeat, wire::encode_heartbeat({}));
    append(pair, framed(wire::msg_type::unidirectional, text("payload")));
    return {framed(wire::msg_type::heartbeat, wire::encode_heartbeat({})), pair, framed(wire::msg_type::rpc_request, {})};
}

seed_set reassembler_seeds()
{
    return {fragment(7, 0, 1, "whole"), fragment(7, 1, 3, "middle"), fragment(9, 2, 3, "")};
}

seed_set varint_seeds()
{
    return {varint_at(1, 0), varint_at(1, 300), varint_at(3, std::numeric_limits<std::uint64_t>::max())};
}

seed_set key_pattern_seeds()
{
    seed_set out;
    for(const std::string_view patterns : {"a/b/c\na/*/c", "a/**/d\na/b/c/d", "robot/*/camera", "lab/**\nlab/arm/**"})
        out.push_back(prefixed({}, text(patterns)));
    return out;
}
