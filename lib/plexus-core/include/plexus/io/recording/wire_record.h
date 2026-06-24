#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_WIRE_RECORD_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_WIRE_RECORD_H

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>

namespace plexus::io::recording {

// The direction plus the per-direction sequence is the cross-node packet-loss join key: a frame
// present in one node's out run and absent from the peer's in run is a structural drop, by
// sequence arithmetic.
enum class wire_direction : std::uint8_t
{
    out = 0,
    in  = 1,
};

// bytes is a borrowed view valid only for the synchronous tap; the recorder copies it into the
// ring before the turn returns. capture_ts is never read here — the encoder sources the timestamp
// from the recorder clock at admit.
struct wire_record
{
    wire_direction dir{wire_direction::out};
    std::uint64_t seq{};
    node_id peer{};
    std::uint64_t capture_ts{};
    std::span<const std::byte> bytes{};
};

}

#endif
