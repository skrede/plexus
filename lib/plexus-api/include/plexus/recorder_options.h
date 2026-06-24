#ifndef HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H
#define HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H

#include "plexus/type_schema.h"

#include "plexus/io/recording/record_envelope.h"

#include "plexus/detail/compat.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus {

// continuous drains every record to the sink as it arrives (drop-newest on overflow);
// pre_buffer holds the last N bytes drop-oldest and dumps the frozen window on a trigger.
enum class recording_mode
{
    continuous = 0,
    pre_buffer = 1,
};

struct recorder_options
{
    using anomaly_predicate = plexus::detail::move_only_function<bool(const io::recording::record_envelope &)>;

    recording_mode mode{recording_mode::continuous};

    // The ring byte-budget. Conservative by design (an overflow is observable while recording,
    // unlike a silent data-plane drop); the ring drains cooperatively and holds only the
    // undrained backlog between executor turns. A firehose capture raises this explicitly.
    std::size_t ring_bytes{1u << 20};

    // A bounded batch the cooperative drain ships per turn, so it never monopolizes one
    // executor turn.
    std::size_t drain_batch_bytes{64u * 1024u};

    // false rides the node's executor turns; plexus never spawns a drain thread unasked.
    bool dedicated_drain_thread{false};

    // The pre_buffer anomaly predicate. Absent freezes only on a manual trigger().
    std::optional<anomaly_predicate> on_anomaly{};

    // Per-type self-descriptions laid into the stream preamble so an offline projector
    // resolves a codec/schema by a sample's type_id. The aliased schema bytes must outlive
    // make_recorder (they are copied into the preamble synchronously in the recorder ctor).
    std::vector<type_schema> schemas{};
};

}

#endif
