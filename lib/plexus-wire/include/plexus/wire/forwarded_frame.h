#ifndef HPP_GUARD_PLEXUS_WIRE_FORWARDED_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_FORWARDED_FRAME_H

#include "plexus/wire/frame.h"

#include "plexus/node_id.h"

#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

// A relayed session verb: it carries an inner frame from an origin toward a destination across one
// relay hop. It rides a NEW session verb (msg_type::forwarded), not a flag bit or an appended
// trailer, because the shipped v0.3.0 unidirectional decoder returns data = reader.rest() and ignores
// nothing — a rider would be delivered inside the subscriber payload (silent corruption). The inner
// region is header-on, so the receive edge re-enters frame_router dispatch for the inner type and one
// verb covers unidirectional AND rpc_request AND rpc_response transit. Decoded straight off an
// untrusted session frame, so decode_forwarded_frame is hardened (latching reader, cap-before-copy,
// ok()-once, nullopt on any malformation, never a partial struct).
struct forwarded_frame
{
    plexus::node_id origin{};
    plexus::node_id destination{};
    std::uint8_t hop   = 0;
    std::uint16_t seq  = 0;
    std::uint8_t flags = 0;
    std::vector<std::byte> inner;

    friend bool operator==(const forwarded_frame &, const forwarded_frame &) = default;
};

// bit0: the origin consents to transitive relay of this frame. Append-only: a new flag takes the next
// free bit and a sender that never sets one emits a byte layout identical to the pre-field one.
inline constexpr std::uint8_t k_forwarded_relay_consent_flag = 0x01;

namespace detail {

// Wire layout: origin(16) + destination(16) + hop(1) + seq(2) + flags(1) + inner_len(4) + inner_bytes.
constexpr std::size_t forwarded_frame_preamble_size = 36;

// Decode-time ceiling on the u32-length-prefixed inner region, held at the reassembler payload cap:
// the inner frame rides inside an outer payload the reassembler already bounds, so decode never
// retains more inner bytes than a whole outer frame could carry.
constexpr std::size_t k_forwarded_inner_max = k_max_reassembler_payload_bytes;

}

}

#include "plexus/wire/detail/forwarded_frame_codec.h"

#endif
