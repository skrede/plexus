#ifndef HPP_GUARD_PLEXUS_IO_LIVENESS_EVENT_H
#define HPP_GUARD_PLEXUS_IO_LIVENESS_EVENT_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

enum class liveness_kind : std::uint8_t
{
    missed_deadline,
    lease_expired
};

struct liveness_event
{
    liveness_kind kind;
    node_id endpoint;
    std::uint64_t topic_hash; // 0 for a session-level lease event (presence is per-endpoint)
    std::uint64_t lapsed_ns;
};

}

#endif
