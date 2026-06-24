#ifndef HPP_GUARD_PLEXUS_IO_OBSERVATION_EVENTS_H
#define HPP_GUARD_PLEXUS_IO_OBSERVATION_EVENTS_H

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/wire/rpc_status.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

using message_view = wire_bytes<>;

struct rpc_view
{
    std::uint64_t correlation_id{};
    message_view param{};
};

struct rpc_reply_view
{
    std::uint64_t correlation_id{};
    wire::rpc_status status{wire::rpc_status::success};
    message_view value{};
};

// Append-only — a new edge takes the next ordinal so a persisted record keeps its meaning.
enum class qos_edge : std::uint8_t
{
    accepted     = 0,
    degraded     = 1,
    refused      = 2,
    unsubscribed = 3,
};

// type_id is engaged only when the subscriber declared one (absence is a distinct third state,
// never a sentinel zero).
struct qos_change_event
{
    qos_edge edge{qos_edge::accepted};
    std::uint64_t topic_hash{};
    node_id peer{};
    subscriber_qos requested{};
    rxo_verdict verdict{rxo_verdict::compatible};
    std::optional<std::uint64_t> type_id{};
};

enum class participant_edge : std::uint8_t
{
    created   = 0,
    destroyed = 1,
};

struct participant_event
{
    participant_edge edge{participant_edge::created};
    node_id self{};
};

// Append-only. The fqn rides as a borrowed on_endpoint parameter, not in the POD.
enum class endpoint_edge : std::uint8_t
{
    publisher_declared    = 0,
    publisher_dropped     = 1,
    subscriber_registered = 2,
    subscriber_retired    = 3,
};

struct endpoint_event
{
    endpoint_edge edge{endpoint_edge::publisher_declared};
    std::uint64_t topic_hash{};
    std::optional<std::uint64_t> type_id{};
};

}

#endif
