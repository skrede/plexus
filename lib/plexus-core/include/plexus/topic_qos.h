#ifndef HPP_GUARD_PLEXUS_TOPIC_QOS_H
#define HPP_GUARD_PLEXUS_TOPIC_QOS_H

#include "plexus/io/locality.h"
#include "plexus/io/priority.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/dispatch_hint.h"

#include <cstdint>

namespace plexus {

struct topic_qos
{
    bool latch                  = false;
    std::uint32_t depth         = 1;
    io::locality reach          = io::locality::any;
    io::reliability reliability = io::reliability::best_effort;
    io::congestion congestion   = io::congestion::block;
    io::priority priority       = io::priority::normal;
    io::dispatch_hint dispatch  = io::dispatch_hint::none;
    // per-topic override of the node message ceiling; 0 = use node default.
    std::uint32_t max_message_bytes = 0;

    // 0 = not offered = always compatible; read producer-side, never on the wire.
    std::uint64_t offered_deadline_ns = 0;
    std::uint64_t offered_lease_ns    = 0;
};

}

#endif
