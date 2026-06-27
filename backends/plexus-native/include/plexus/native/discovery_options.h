#ifndef HPP_GUARD_PLEXUS_NATIVE_DISCOVERY_OPTIONS_H
#define HPP_GUARD_PLEXUS_NATIVE_DISCOVERY_OPTIONS_H

#include <string>
#include <chrono>
#include <cstdint>

namespace plexus::native {

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
            , port(7447)
            , ttl(4)
    {
    }

    std::string group;
    std::chrono::milliseconds announce_period; // a conservative starting value, tuned empirically later
    std::uint16_t port;
    unsigned ttl;
};

}

#endif
