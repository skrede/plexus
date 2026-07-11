#ifndef HPP_GUARD_PLEXUS_NODE_OPTIONS_H
#define HPP_GUARD_PLEXUS_NODE_OPTIONS_H

#include "plexus/io/fragmentation.h"
#include "plexus/io/host_fingerprint.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/log/logger.h"

#include "plexus/recording_qos.h"
#include "plexus/wire_capture_qos.h"

#include <string>
#include <chrono>
#include <cstdint>

namespace plexus {

// The version pair a node advertises and the minimum it accepts. A null (zero)
// local_fingerprint is the meaningful "no same-host signal" value, never a sentinel.
struct handshake_options
{
    std::uint8_t version_major{1};
    std::uint8_t version_minor{0};
    std::uint8_t compatible_version_major{1};
    std::uint8_t compatible_version_minor{0};
    io::host_fingerprint local_fingerprint{};
};

// The hot-path substrate is borrowed by reference at construction, so this aggregate carries
// only plain values and the two borrowed null-object handles (attach_policy, logger).
struct node_options
{
    // Empty falls back to io::node_name_of(id) for the discovery service name.
    std::string name;

    handshake_options handshake;

    std::chrono::nanoseconds handshake_timeout{std::chrono::seconds(5)};

    // 0 is today's abort-on-timeout; a non-zero count re-sends a lost handshake request that many
    // times (each spaced one handshake_timeout window) before surrendering to the same abort. Only a
    // directly-dialed point-to-point host (no card to redial off) needs it set; it is node-WIDE.
    std::uint32_t handshake_retry{0};

    // REQUIRED: the reconnect backoff/surrender cadence is tuned per deployment, so a zeroed
    // default would silently ship an un-tuned cadence.
    io::reconnect_config reconnect;

    // REQUIRED, distinct per node — a shared seed phase-locks redial jitter across nodes,
    // defeating the decorrelation the jitter exists to provide.
    std::uint64_t redial_seed{};

    // false is LAZY (note_peer records awareness, dials on demand); true dials off note_peer.
    bool dial_eagerly{false};

    // Borrowed for the engine's lifetime. Null is accept-any (the no-PSK default).
    const io::security::attach_policy *attach_policy{nullptr};

    // The cold-path log sink, borrowed. Null resolves to the node-owned inert default logger.
    log::logger *logger{nullptr};

    // The node-level per-message size default a topic with no override negotiates against.
    std::size_t max_message_bytes{io::global_default_max_message_bytes};

    // The node-scoped liveliness policy group: awareness-aging deadline, heartbeat interval,
    // heartbeat miss limit, and the fusion policy. Consumer-tunable; each default's provenance
    // (the carried aging deadline vs the interim heartbeat values) is documented at the
    // liveliness_options definition.
    io::liveliness_options liveliness{};

    // fidelity off selects nothing, so a node that declares no recording QoS ships zero capture.
    recording_qos capture{};

    // The decorated-vs-bare channel type is fixed by the policy the node is composed over; this
    // field declares the consumer's intent and crypto position. Disabled ships no wire capture.
    wire_capture_qos wire{};
};

}

#endif
