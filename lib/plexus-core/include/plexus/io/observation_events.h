#ifndef HPP_GUARD_PLEXUS_IO_OBSERVATION_EVENTS_H
#define HPP_GUARD_PLEXUS_IO_OBSERVATION_EVENTS_H

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/wire/rpc_status.h"

#include "plexus/wire_bytes.h"
#include "plexus/node_id.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

// The byte view the message/rpc taps surface: the non-owning span plus the
// Policy-selected owner whose lifetime bounds it (the SAME carrier the receive seam
// hands up). The tap borrows that owner — a delivered view shares the buffer the
// channel surfaced, never a fresh copy.
using message_view = wire_bytes<>;

// The argument bytes a request-tap surfaces: the call's correlation id plus the
// borrowed parameter view. No closure, no owned payload beyond the shared handle.
struct rpc_view
{
    std::uint64_t correlation_id{};
    message_view  param{};
};

// The result bytes a reply-tap surfaces: the correlation id the reply answers, the
// wire-stable status byte, and the borrowed return view.
struct rpc_reply_view
{
    std::uint64_t      correlation_id{};
    wire::rpc_status   status{wire::rpc_status::success};
    message_view       value{};
};

// The QoS transition a subscriber attach resolved to. The edges name where in the
// attach a verdict landed: an offered topic admitted cleanly, admitted degraded, or
// refused. Append-only — a new edge takes the next ordinal so a persisted record
// keeps its meaning.
enum class qos_edge : std::uint8_t
{
    accepted = 0,
    degraded = 1,
    refused  = 2,
};

// A plain serializable QoS-change record in the drop_event spirit: scalars and a
// node_id, no closure-with-context, so it timestamps, serializes, and multiplexes
// onto a future event spine without preclusion. type_id is engaged only when the
// subscriber declared one (absence is a distinct third state, never a sentinel zero).
struct qos_change_event
{
    qos_edge                     edge{qos_edge::accepted};
    std::uint64_t                topic_hash{};
    node_id                      peer{};
    subscriber_qos               requested{};
    rxo_verdict                  verdict{rxo_verdict::compatible};
    std::optional<std::uint64_t> type_id{};
};

}

#endif
