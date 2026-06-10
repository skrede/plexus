#ifndef HPP_GUARD_PLEXUS_IO_LIVENESS_EVENT_H
#define HPP_GUARD_PLEXUS_IO_LIVENESS_EVENT_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

// Which timing gate a fired liveness event reports. missed_deadline is an inter-data
// gap on a (endpoint, topic) that exceeded the subscriber's requested deadline;
// lease_expired is a presence gap (no data AND no heartbeat) on an endpoint that
// exceeded the subscriber's requested lease.
enum class liveness_kind : std::uint8_t
{
    missed_deadline,
    lease_expired
};

// The observable a missed-deadline / lease-expiry carries up the subscriber-side
// callback. It is a dedicated timing-gate event (not a serialization artifact and
// not a lifecycle edge): the monitor fires it through a settable callback the engine
// routes to its observer seam. topic_hash names the lapsed topic for a deadline
// event; it is 0 for a session-level lease event (presence is keyed per-endpoint).
// lapsed_ns carries the period that was exceeded.
struct liveness_event
{
    liveness_kind kind;
    node_id       endpoint;
    std::uint64_t topic_hash;
    std::uint64_t lapsed_ns;
};

}

#endif
