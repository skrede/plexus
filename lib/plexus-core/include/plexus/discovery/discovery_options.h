#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_OPTIONS_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_OPTIONS_H

#include "plexus/io/network_interface.h"

#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::discovery {

// The admission cap for the open (unauthenticated) multicast group. max_peers bounds the distinct
// admitted-source set; 256 is far below the data-plane demux's 4096 default since a LAN-segment
// awareness table is small and a conservative bound limits a distinct-source flood's footprint.
// per_source_max/per_source_window bound one source's announce rate (4 per 1s tolerates the 1s
// announce cadence plus a small burst). evict_lru off by default protects an established peer from
// being evicted by a flood of new sources. Sentinels: max_peers == 0 is unlimited, per_source_max
// == 0 disables the rate limit; both zero disables admission gating entirely.
struct flood_cap_options
{
    flood_cap_options()
            : per_source_window(1000)
            , max_peers(256)
            , per_source_max(4)
            , evict_lru(false)
    {
    }

    std::chrono::milliseconds per_source_window;
    std::size_t max_peers;
    std::size_t per_source_max;
    bool evict_lru;
};

// The multicast discovery group/port/ttl and re-announce cadence. The group is held as a string so
// this header stays OS-free and asio-free (the caller-supplied socket parses it); 239.255.0.7 is
// admin-scoped, distinct from the DDS SPDP 239.255.0.1 so a co-resident DDS deployment never
// collides, and 7447 is clear of mDNS 5353, SSDP 1900, and the DDS 7400 range. TTL>1 is routable
// across a few LAN segments. Every field is overridable down to link-local (224.0.0.x / TTL 1).
struct discovery_options
{
    discovery_options()
            : group("239.255.0.7")
            , announce_period(1000)
            , cap()
            , egress_interface(io::network_interface::any())
            , port(7447)
            , ttl(4)
    {
    }

    std::string group;
    std::chrono::milliseconds announce_period; // a conservative starting value, tuned empirically later
    flood_cap_options cap;
    io::network_interface egress_interface;
    std::uint16_t port;
    unsigned ttl;
};

}

#endif
