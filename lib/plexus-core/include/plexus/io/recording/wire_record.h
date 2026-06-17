#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_WIRE_RECORD_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_WIRE_RECORD_H

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>

namespace plexus::io::recording {

// Which side of the channel a captured frame crossed. out is a send() the decorator
// taps before forwarding verbatim; in is a frame the lower channel delivered upward
// before the decorator re-emits it. The direction plus the per-direction sequence is
// the cross-node packet-loss join key (a frame present in one node's out run and
// absent from the peer's in run is a structural drop, by sequence arithmetic).
enum class wire_direction : std::uint8_t
{
    out = 0,
    in  = 1,
};

// One captured wire frame in flight to the recorder: the direction, the per-direction
// sequence, the peer identity, the capture instant, and the full framed bytes byte-
// identical to what the lower channel sent or received. bytes is a borrowed view valid
// only for the synchronous tap; the recorder copies it into the ring before the turn
// returns. capture_ts is carried for symmetry with the decoded form — the encoder
// sources the timestamp from the recorder clock at admit, so it is never read here.
struct wire_record
{
    wire_direction             dir{wire_direction::out};
    std::uint64_t              seq{};
    node_id                    peer{};
    std::uint64_t              capture_ts{};
    std::span<const std::byte> bytes{};
};

}

#endif
