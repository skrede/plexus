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

// Which discipline the attached recorder runs on its one byte_ring: continuous drains
// every record to the sink as it arrives (drop-newest on overflow, the always-on black
// box); pre_buffer holds "the last N bytes" drop-oldest and dumps the frozen window only
// when a trigger fires (the aviation-FDR posture).
enum class recording_mode
{
    continuous = 0,
    pre_buffer = 1,
};

// The construction-time options for node.make_recorder. The byte-budget is a conservative
// host default (recall = min(1, ring_bytes / backlog), so the budget sizes the bytes of
// transient producer backlog the always-on black box absorbs at full recall); a deployment
// raises it for a firehose. The cooperative drain rides the node's
// run-loop by default (a self-re-posting drain task — no plexus thread); a dedicated drain
// thread is an explicit consumer opt-in, never the default. For the pre_buffer mode an
// optional anomaly predicate auto-freezes the window when it matches a built
// record_envelope (absence is meaningful — no predicate means manual trigger() only),
// typed fully-qualified to mirror the core seam.
struct recorder_options
{
    using anomaly_predicate =
        plexus::detail::move_only_function<bool(const io::recording::record_envelope &)>;

    recording_mode mode{recording_mode::continuous};

    // The ring byte-budget. Conservative by design: recording resources are spent
    // sparingly (an overflow is observable while recording, unlike a silent data-plane
    // drop), so the default holds the common small-payload case rather than a firehose. The
    // ring drains cooperatively — it holds only the undrained backlog between executor
    // turns, not a whole session — so 1 MiB absorbs ~1,900 records of a ~512 B backlog, full
    // recall for the typical small-payload traffic and shedding only under a genuine
    // saturating burst (itself observable while recording). A large-payload / firehose
    // capture raises this explicitly; per-topic budgeting composes one recorder per
    // topic-group.
    std::size_t ring_bytes{1u << 20};

    // required-with-default: a bounded batch the cooperative drain ships per turn before
    // yielding, so a drain never monopolizes one executor turn. Recall is drain-cadence-
    // independent (the sweep moves it with ring_bytes, not this batch), so this governs
    // per-turn executor fairness rather than steady-state recall.
    std::size_t drain_batch_bytes{64u * 1024u};

    // required-with-default false: the drain rides the node's executor turns. A consumer
    // who wants a dedicated drain thread opts in explicitly; plexus never spawns one.
    bool dedicated_drain_thread{false};

    // std::optional: the FDR anomaly predicate (pre_buffer mode). Absence is meaningful —
    // a recorder with no predicate freezes only on a manual trigger().
    std::optional<anomaly_predicate> on_anomaly{};

    // required-with-default empty: the per-type self-descriptions the recorder lays into the
    // stream preamble so an offline projector resolves a codec/schema by a sample's type_id.
    // Empty is the meaningful default — a recorder that declares nothing still writes a valid
    // opaque stream (the MCU floor). The aliased schema bytes must outlive make_recorder
    // (they are copied into the preamble synchronously in the recorder ctor).
    std::vector<type_schema> schemas{};
};

}

#endif
