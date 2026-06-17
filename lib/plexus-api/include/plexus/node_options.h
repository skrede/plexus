#ifndef HPP_GUARD_PLEXUS_NODE_OPTIONS_H
#define HPP_GUARD_PLEXUS_NODE_OPTIONS_H

#include "plexus/io/fragmentation.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/security/attach_policy.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_geometry_mode.h"

#include "plexus/log/logger.h"

#include "plexus/recording_qos.h"

#include <string>
#include <chrono>
#include <cstdint>

namespace plexus {

// The version pair a node advertises and the minimum it accepts. All four fields
// are required-with-default {1,0,1,0}: a zeroed default is a usable advertisement
// (an honest version-zero node), so the default carries meaning rather than
// standing in for absence. local_fingerprint is required-with-default too — a null
// (zero) fingerprint is the meaningful "no same-host signal" value (a node with no
// shared-memory backend is correctly never co-located), not a sentinel for absence.
struct handshake_options
{
    std::uint8_t version_major{1};
    std::uint8_t version_minor{0};
    std::uint8_t compatible_version_major{1};
    std::uint8_t compatible_version_minor{0};
    io::shm::host_fingerprint local_fingerprint{};
};

// Every node tunable in one plain, non-templated, designated-initializer-friendly
// aggregate. Nothing here is Policy-typed: the hot-path substrate is borrowed by
// reference at construction, so the options carry only plain values and the two
// null-object virtual/abstract handles (attach_policy, logger). Each field follows
// the three-way discipline (required / required-with-default / std::optional only
// where absence is itself meaningful).
struct node_options
{
    // required-with-default "": empty means the discovery service name falls back to
    // io::node_name_of(id) (the existing display-name derivation). A non-empty name
    // is a deliberate human-facing service label, never a substitute for the id.
    std::string name;

    // required-with-default {1,0,1,0}: the conventional matched pair every node ships
    // unless it deliberately advertises or requires a different protocol version.
    handshake_options handshake;

    // required-with-default 5s: a conventional connect/handshake liveness bound. It is
    // a duration, NOT a std::optional<duration> — its absence is not meaningful, only
    // its value is. (Operator review item: the right production bound is deployment-
    // specific; 5s is a liveness default, not a tuned ceiling.)
    std::chrono::nanoseconds handshake_timeout{std::chrono::seconds(5)};

    // REQUIRED, no meaningful default: the reconnect backoff/surrender cadence is
    // empirically tuned per deployment (reconnect_config's own min/max_delay carry no
    // default for the same reason), so a zeroed default would silently ship an
    // un-tuned cadence. The caller must supply it.
    io::reconnect_config reconnect;

    // REQUIRED: the per-node redial jitter seed. It must be distinct per node — a seed
    // shared across nodes phase-locks their redial jitter, defeating the decorrelation
    // the jitter exists to provide. Documented-required, no silent default.
    std::uint64_t redial_seed{};

    // required-with-default false (LAZY): note_peer records awareness and dials only on
    // demand. true (EAGER) dials immediately off note_peer. A bool, NOT a
    // std::optional<bool> — its absence is not meaningful, only its value is.
    bool dial_eagerly{false};

    // The node-level admission gate, borrowed (the node owns it for the engine's
    // lifetime). A null policy is accept-any — the explicit no-PSK default that
    // preserves the pre-attach-gate behavior.
    const io::security::attach_policy *attach_policy{nullptr};

    // The cold-path log sink, borrowed. Null resolves to shared_null_logger() once in
    // the node ctor. A null-object default pointer, never a stand-in for a real sink.
    log::logger *logger{nullptr};

    // required-with-default 8 MiB: the node-level per-message size default a topic with
    // no per-message override negotiates against (effective_max). It is the SINGLE
    // source the RxO check and the data-path resolve against; the shipped constant is
    // the meaningful default, not a sentinel for absence.
    std::size_t max_message_bytes{io::global_default_max_message_bytes};

    // required-with-default {0, reliable_preserving}: the node-level same-host ring
    // geometry default a publisher with no per-topic override resolves against.
    // max_consumers 0 = unset (resolves to the shipped capacity floor); the default
    // mode is the safe reliable ring. A plain value (not std::optional): a zeroed
    // default is a usable geometry, its absence is not meaningful.
    io::shm::shm_geometry shm_geometry{};

    // required-with-default 16 MiB: the node-level per-ring slab ceiling the SHM
    // registry enforces — operator-locked, a TUNABLE knob, not a fixed compile-time
    // literal. The shipped constant is the meaningful default the registry falls back
    // to; a deployment raises it for a larger reliable ring.
    std::uint64_t max_ring_slab_bytes{io::shm::k_max_ring_slab_bytes};

    // required-with-default off: the node-level recording fidelity a topic with no
    // per-topic override falls back to. fidelity off is the meaningful default — it
    // SELECTS NOTHING, so a node that declares no recording QoS ships zero capture and
    // the gate stays fully inert. A plain value (not std::optional): the off default is
    // a usable declaration, its absence is not meaningful.
    recording_qos capture{};
};

}

#endif
