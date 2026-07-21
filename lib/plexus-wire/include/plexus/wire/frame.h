#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_H

#include <limits>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

constexpr std::size_t header_size = 28;

// Default upper bound on payload_len: a denial-of-service cap on untrusted input. The
// static_assert pins it below INT_MAX so the byte-count stays representable as a signed int.
constexpr std::size_t k_max_reassembler_payload_bytes = 16 * 1024 * 1024;
static_assert(k_max_reassembler_payload_bytes < std::numeric_limits<int>::max(),
              "k_max_reassembler_payload_bytes must stay below INT_MAX so the "
              "reassembled byte-count is representable as a signed int");

constexpr std::byte magic_byte_0{0x56};
constexpr std::byte magic_byte_1{0x50};

// Wire-stable message-type byte. Append-only: a value is NEVER reordered or reused; a new
// msg_type takes the next free integer (0x0D+) and is byte-identical to a peer that never
// emits it, so it rides WITHIN the current protocol version with no bump.
enum class msg_type : uint8_t
{
    unidirectional     = 0x01,
    bidirectional      = 0x02,
    handshake_req      = 0x03,
    handshake_resp     = 0x04,
    subscribe          = 0x05,
    unsubscribe        = 0x06,
    fetch_latched      = 0x07,
    fetch_metadata     = 0x08,
    rpc_request        = 0x09,
    rpc_response       = 0x0A,
    subscribe_response = 0x0B,
    heartbeat          = 0x0C,
    declare            = 0x0D,
    peer_report        = 0x0E,
    forwarded          = 0x0F
};

enum class endpoint_source_type : uint8_t
{
    publisher = 0x01,
    signal    = 0x03,
    attribute = 0x05,
    caller    = 0x06,
    procedure = 0x07,
    plexus    = 0x08
};

// frame_header.flags bit allocation. Append-only: a new flag takes the next free bit; a frame
// with a bit CLEAR is byte-identical to a frame that never sets it. k_flag_source_identity
// signals a flag-gated varint endpoint counter appended after the fixed header (the receiver
// reconstructs the publisher_gid as session.node_id ‖ counter). The bit is RESERVED — the
// layout is specified but the varint region is not yet encoded or decoded. Bit 0x02 is RESERVED
// for a future on-wire priority/control signal; bits 0x04..0x80 remain free.
constexpr std::uint8_t k_flag_source_identity = 0x01;

struct frame_header
{
    msg_type type;
    uint8_t flags;
    // Monotonic session-id minted on each handshake completion. 0 == unestablished sentinel:
    // handshake control frames carry 0; the receiver-side staleness gate latches the first
    // non-zero observation and drops frames whose session_id differs. Drawn from a u64 epoch
    // well that wraps back to 1 (never 0) to preserve the sentinel.
    uint64_t session_id;
    uint64_t timestamp_ns;
    uint64_t payload_len;
};

}

#endif
