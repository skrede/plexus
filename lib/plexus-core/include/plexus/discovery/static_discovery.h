#ifndef HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H

#include "plexus/discovery/discovery.h"

#include "plexus/io/endpoint.h"

#include <string>
#include <vector>
#include <utility>
#include <algorithm>

namespace plexus::discovery {

// Fixed-table discovery. browse() retains its callback so a later advertise()
// notifies every live browser; advertise() replaces a same-name entry in place.
class static_discovery final : public discovery
{
public:
    explicit static_discovery(std::vector<service_info> table)
            : m_table(std::move(table))
    {
    }

    void advertise(const service_info &service) override
    {
        const auto it = std::find_if(m_table.begin(), m_table.end(), [&](const service_info &e) { return e.name == service.name; });
        if(it != m_table.end())
            *it = service;
        else
            m_table.push_back(service);
        for(const auto &browser : m_browsers)
            browser(service);
    }

    void browse(const resolved_callback &on_resolved) override
    {
        for(const auto &entry : m_table)
            on_resolved(entry);
        m_browsers.push_back(on_resolved);
    }

    void stop() override
    {
        m_browsers.clear();
    }

private:
    std::vector<service_info> m_table;
    std::vector<resolved_callback> m_browsers;
};

}

#endif
