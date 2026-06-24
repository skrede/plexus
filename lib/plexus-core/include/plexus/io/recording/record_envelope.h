#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_ENVELOPE_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_ENVELOPE_H

#include "plexus/io/detail/drop_event.h"

#include <cstdint>

namespace plexus::io::recording {

// The ordinals are wire-stable and append-only: a persisted stream keeps its meaning across
// versions, so a new kind takes the next free ordinal and an existing one is NEVER renumbered or
// removed.
enum class record_category : std::uint8_t
{
    sample        = 0,
    drop          = 1,
    qos_change    = 2,
    participant   = 3,
    endpoint      = 4,
    rpc_call      = 5,
    rpc_serve     = 6,
    rpc_reply     = 7,
    security      = 8,
    peer_liveness = 9,
    dropout       = 10,
    wire_frame    = 11,
};

struct record_envelope
{
    record_category category{record_category::sample};
    std::uint64_t capture_ts{};
    std::uint64_t topic_hash{};
    io::detail::drop_cause cause{io::detail::drop_cause::none};
    std::uint8_t verdict{};
};

}

#endif
