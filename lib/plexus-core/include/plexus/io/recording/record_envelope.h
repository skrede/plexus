#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_ENVELOPE_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_ENVELOPE_H

#include "plexus/io/detail/drop_event.h"

#include <cstdint>

namespace plexus::io::recording {

// The leading discriminator of every record in the flat stream. Each enumerator
// names a plexus-native record kind that maps to one observer edge (a sample, the
// session/declaration lifecycle edges, the QoS/drop/security taps), plus the
// recorder's own honesty record. The ordinals are wire-stable and append-only — a
// persisted stream keeps its meaning across versions, so a new kind takes the next
// free ordinal and an existing one is NEVER renumbered or removed.
//
// wire_frame is RESERVED now and unpopulated: the wire-fidelity tier carries the raw
// on-the-wire frame bytes (the encrypted/framed transport form), a later source the
// format reserves a slot for today so adding it changes only the byte SOURCE, with no
// format change and no version bump. Until that source lands a wire-fidelity request
// degrades to a sample record.
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

// A non-owning view of a record at the moment it is built, the shape a freeze
// predicate keys on (the "trigger on anomaly" black-box semantic). It carries only
// the scalar facts a predicate matches — the category, the capture instant, the topic
// identity, and the drop cause / qos verdict an anomaly edge surfaces — so evaluating
// it on the record-build turn copies nothing and allocates nothing. The raw payload is
// never exposed here: a freeze fires on an edge's shape, not its bytes.
struct record_envelope
{
    record_category   category{record_category::sample};
    std::uint64_t     capture_ts{};
    std::uint64_t     topic_hash{};
    io::detail::drop_cause cause{io::detail::drop_cause::none};
    std::uint8_t      verdict{};
};

}

#endif
