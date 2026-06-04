#ifndef HPP_GUARD_PLEXUS_TOPIC_QOS_H
#define HPP_GUARD_PLEXUS_TOPIC_QOS_H

#include <cstdint>

namespace plexus {

// The publisher-declared per-topic QoS — the first plexus QoS type. Deliberately
// minimal: a latch/retain flag and a retain depth (1 today, the last value).
// `latch` makes the publisher retain the last published value and replay it to
// late subscribers; `depth` is the retained-history depth, the seam for last-N
// retrieval later. Reliability / durability / deadline / overflow are upstream
// concepts NOT carried here — the struct is shaped so they append later as future
// fields (default = "unset") WITHOUT reshaping the call sites. Plain fields with
// defaults: plexus has no QoS negotiation, so there is no "unset" to distinguish.
struct topic_qos
{
    bool          latch = false;
    std::uint32_t depth = 1;
};

}

#endif
