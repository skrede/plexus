#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_H

#include "plexus/io/endpoint.h"

#include <string>
#include <functional>

namespace plexus::discovery {

// A discoverable service: a name and the endpoint a peer reaches it at. This is
// the unit both advertise (publish my service) and the resolved-callback (a peer
// I found) carry.
struct service_info
{
    std::string name;
    io::endpoint endpoint;
};

// Cold-path runtime discovery interface — the first of the two locked virtual
// seams (the other is logger). It is a runtime-injected abstract base, NOT a
// Policy template member, so the backend (static table, mDNS via mdnspp, a
// broker) is swapped without touching the hot-path Policy. All arguments pass by
// const& (no raw pointers, per project convention).
class discovery
{
public:
    using resolved_callback = std::function<void(const service_info &)>;

    virtual ~discovery() = default;

    // Publish a local service so peers can resolve it.
    virtual void advertise(const service_info &service) = 0;

    // Start resolving peers; on_resolved is invoked once per discovered service.
    virtual void browse(const resolved_callback &on_resolved) = 0;

    // Stop advertising/browsing.
    virtual void stop() = 0;
};

}

#endif
