#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_H

#include <limits>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

constexpr std::size_t header_size = 28;

// Maximum reassembled-frame payload size. The reassembler's ctor reads this
// as the default upper bound on payload_len. plexus is serializer-agnostic and
// moves opaque bytes, so the cap exists purely as a denial-of-service bound on
// untrusted input. The static_assert pins it below INT_MAX so the byte-count
// stays representable as a signed int across every downstream consumer that
// hands the span to a size-parameterized API.
constexpr std::size_t k_max_reassembler_payload_bytes = 16 * 1024 * 1024;
static_assert(k_max_reassembler_payload_bytes < std::numeric_limits<int>::max(),
              "k_max_reassembler_payload_bytes must stay below INT_MAX so the "
              "reassembled byte-count is representable as a signed int");

constexpr std::byte magic_byte_0{0x56};
constexpr std::byte magic_byte_1{0x50};

enum class msg_type : uint8_t
{
    unidirectional      = 0x01,
    bidirectional       = 0x02,
    handshake_req       = 0x03,
    handshake_resp      = 0x04,
    subscribe           = 0x05,
    unsubscribe         = 0x06,
    fetch_latched       = 0x07,
    fetch_metadata      = 0x08,
    rpc_request         = 0x09,
    rpc_response        = 0x0A,
    subscribe_response  = 0x0B
};

enum class endpoint_source_type : uint8_t
{
    publisher      = 0x01,
    signal         = 0x03,
    attribute      = 0x05,
    caller         = 0x06,
    procedure      = 0x07,
    plexus         = 0x08
};

// frame_header.flags bit allocation. The flags byte was always written 0 in
// v0.1.x; this is the FIRST allocated bit. k_flag_source_identity signals that a
// flag-gated, varint-encoded endpoint counter is appended after the fixed header
// (the receiver reconstructs the publisher_gid as session.node_id ‖ counter). The
// bit is RESERVED here so the v3 layout is fully specified, but the varint region
// is not yet encoded or decoded: a frame with the bit CLEAR is byte-identical to a
// freshly-bumped v3 no-flag frame. Append-only: a new flag takes the next free bit.
constexpr std::uint8_t k_flag_source_identity = 0x01;

struct frame_header
{
    msg_type type;
    uint8_t  flags;
    // Monotonic session-id minted on each successful handshake completion.
    // 0 == unestablished sentinel: handshake control frames carry 0; the
    // receiver-side staleness gate latches the first non-zero observation
    // and drops subsequent frames whose session_id differs. Drawn from a u64
    // epoch well that wraps back to 1 (never 0) to preserve the sentinel
    // semantics; the u64 width retires the 255-reconnect wrap window a reused
    // slot's u8 epoch cycled through. It does not by itself fix an accepted slot
    // that restarts its epoch from 1 on re-accept — that is a separate slot-reuse
    // concern, untouched here.
    uint64_t session_id;
    uint64_t timestamp_ns;
    uint64_t payload_len;
};

}

#endif
