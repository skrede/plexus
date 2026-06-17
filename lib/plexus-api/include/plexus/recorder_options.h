#ifndef HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H
#define HPP_GUARD_PLEXUS_API_RECORDER_OPTIONS_H

#include "plexus/io/recording/record_envelope.h"

#include "plexus/detail/compat.h"

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

// The construction-time options for node.make_recorder. The byte-budget is the host
// default substantiated by the recorded sweep (recall = min(1, ring_bytes / backlog), so
// the budget sizes the bytes of transient producer burst the always-on black box absorbs
// at full recall); a deployment overrides it. The cooperative drain rides the node's
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

    // The ring byte-budget. The host default holds full recall at every measured payload
    // size for a multi-thousand-message transient (recorder_sweep / test_recorder_defaults_sweep
    // record the recall-vs-budget curve and guard this floor); the MCU re-tunes it down.
    std::size_t ring_bytes{16u * 1024u * 1024u};

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
};

}

#endif
