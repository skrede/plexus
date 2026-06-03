#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_H

#include <limits>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

constexpr std::size_t header_size = 21;

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

struct frame_header
{
    msg_type type;
    uint8_t  flags;
    // Monotonic session-id minted on each successful handshake completion.
    // 0 == unestablished sentinel: handshake control frames carry 0; the
    // receiver-side staleness gate latches the first non-zero observation
    // and drops subsequent frames whose session_id differs. Wraps from 255
    // back to 1 (never 0) to preserve the sentinel semantics.
    uint8_t  session_id;
    uint64_t timestamp_ns;
    uint64_t payload_len;
};

constexpr uint8_t flag_relay = 0x01;

}

#endif
