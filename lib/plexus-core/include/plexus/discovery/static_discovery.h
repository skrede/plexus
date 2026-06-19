#ifndef HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H

#include "plexus/discovery/discovery.h"

#include "plexus/io/endpoint.h"

#include <string>
#include <vector>
#include <utility>
#include <algorithm>

namespace plexus::discovery {

// Live fixed-table discovery: a developer-provided {name -> service} table, no real
// mDNS. browse() fires the callback once per CURRENT entry and RETAINS the callback,
// so a later advertise() notifies every retained browser live (a late joiner is seen
// without a re-browse). advertise() replaces a same-name entry in place rather than
// appending a duplicate, so re-advertising a node updates its record. The table and
// the browsers are trusted developer input, so there is no untrusted network parsing
// here. stop() drops the retained browsers.
class static_discovery final : public discovery
{
public:
    explicit static_discovery(std::vector<service_info> table)
            : m_table(std::move(table))
    {
    }

    void advertise(const service_info &service) override
    {
        const auto it = std::find_if(m_table.begin(), m_table.end(),
                                     [&](const service_info &e) { return e.name == service.name; });
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

    void stop() override { m_browsers.clear(); }

private:
    std::vector<service_info>      m_table;
    std::vector<resolved_callback> m_browsers;
};

}

#endif
