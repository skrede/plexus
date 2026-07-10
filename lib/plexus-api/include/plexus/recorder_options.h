#ifndef HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H
#define HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H

#include "plexus/type_schema.h"

#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_rate_gate.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/detail/compat.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

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

    // Per-topic record-rate rules keyed by topic_hash, applied before the ring push: max-Hz
    // throttling, every-Nth decimation, and per-topic enable/disable. Absent for a topic records
    // every sample. Bounds logging cost without a data-plane drop.
    std::vector<std::pair<std::uint64_t, io::recording::record_rate_rule>> record_rates{};

    // Add a per-topic record-rate rule by fully-qualified name (a cold-path convenience over the
    // topic_hash-keyed record_rates vector).
    void set_record_rate(std::string_view fqn, io::recording::record_rate_rule rule)
    {
        record_rates.emplace_back(wire::fqn_topic_hash(fqn), rule);
    }
};

}

#endif
