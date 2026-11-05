#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_DROP_EVENT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_DROP_EVENT_H

#include "plexus/io/locality.h"
#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io::detail {

// Append-only: a wire-stable counter index and persisted records depend on each ordinal
// staying fixed, so new causes extend the tail and never reorder existing values.
enum class drop_cause : std::uint8_t
{
    none = 0,
    drop_oldest,
    drop_newest,
    blocked,
    replay,
    too_old,
    tamper,
    reassembly_evicted,
    reassembly_cap,
    malformed,
    demux_refused,
    arq_shed,
    unroutable,
    closed_unsent,
    splice_exhausted,
    splice_oversize,
    // A frame larger than its outbound leg's per-message ceiling, refused before it reached the wire.
    oversize,
    unestablished,
    foreign_session,
    // A subscribe whose bounded re-sends all went unanswered; the session is kept.
    unacknowledged,
    // Data for a topic this node no longer demands from that peer; the unsubscribe is re-sent.
    unsolicited,
};

struct drop_event
{
    drop_cause cause{drop_cause::none};
    locality transport{locality::any};
    std::uint8_t band{0};
    std::uint64_t topic_hash{0};
    node_id peer{};
    std::uint64_t count{1};
};

}

#endif
