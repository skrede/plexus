#ifndef HPP_GUARD_PLEXUS_TOPIC_QOS_H
#define HPP_GUARD_PLEXUS_TOPIC_QOS_H

#include "plexus/io/locality.h"
#include "plexus/io/priority.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/shm/dispatch_hint.h"

#include <cstdint>

namespace plexus {

// The publisher-declared per-topic QoS — the first plexus QoS type. Deliberately
// minimal: a latch/retain flag, a retain depth (1 today, the last value), and a
// delivery-tier confinement mask.
// `latch` makes the publisher retain the last published value and replay it to
// late subscribers; `depth` is the retained-history depth, the seam for last-N
// retrieval later. `reach` is the delivery-tier confinement field: a mask of the
// localities a topic's bytes may travel to (default = any, so an undeclared topic
// reaches every tier — no confinement); a confined mask drops a send/subscribe to
// any out-of-scope tier. `reliability` selects the lossy best-effort class or the
// in-order reliable class; `congestion` selects what a full send path does (shed
// at the publisher, or back-pressure the publish); `priority` is the LOCAL,
// off-wire egress-band selector — under a back-pressured destination a higher-
// priority topic's frames drain ahead of a lower one's (default normal, a middle
// band). The defaults carry the safe
// per-class intent: best_effort (the lossy class) with block (so a reliable topic
// that overrides reliability inherits a guarantee-preserving back-pressure rather
// than a silent drop). Durability / deadline / overflow are upstream concepts NOT
// carried here — the struct is shaped so they append later as future fields
// (default = "unset") WITHOUT reshaping the call sites. Plain fields with defaults:
// plexus has no QoS negotiation, so there is no "unset" to distinguish.
//
// `dispatch` is the shared-memory eligibility hint (a FLAGS bitmask; none = 0 is
// the absence): a same-host topic with any bit set prefers the shared-memory
// medium, none keeps it on the local stream. `max_payload` (0 = unset) is the
// publisher's ring-sizing authority for that medium: it sizes the broadcast ring's
// slot width; 0 falls back to the default ring geometry (the case for a
// subscriber-only upgrade, where no publisher declared a width).
struct topic_qos
{
    bool                  latch       = false;
    std::uint32_t         depth       = 1;
    io::locality          reach       = io::locality::any;
    io::reliability       reliability = io::reliability::best_effort;
    io::congestion        congestion  = io::congestion::block;
    io::priority          priority    = io::priority::normal;
    io::shm::dispatch_hint dispatch   = io::shm::dispatch_hint::none;
    std::uint32_t         max_payload = 0;
};

}

#endif
