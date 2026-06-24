#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_H

#include "plexus/io/endpoint.h"

#include <string>
#include <vector>
#include <utility>
#include <functional>

namespace plexus::discovery {

struct service_info
{
    std::string name;
    io::endpoint endpoint;
    // ordered for deterministic round-trips; a backend maps it onto its advertisement.
    std::vector<std::pair<std::string, std::string>> metadata;
};

class discovery
{
public:
    using resolved_callback = std::function<void(const service_info &)>;

    virtual ~discovery() = default;

    virtual void advertise(const service_info &service) = 0;

    virtual void browse(const resolved_callback &on_resolved) = 0;

    virtual void stop() = 0;
};

}

#endif
