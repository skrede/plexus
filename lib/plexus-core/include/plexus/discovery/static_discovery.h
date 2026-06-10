#ifndef HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_STATIC_DISCOVERY_H

#include "plexus/discovery/discovery.h"

#include "plexus/io/endpoint.h"

#include <string>
#include <utility>
#include <vector>

namespace plexus::discovery {

// Fixed-table discovery fallback: resolves names from a developer-provided
// {name -> endpoint} table built at construction, no real mDNS. browse() fires
// the callback once per seeded entry. This is the no-mDNS path the round-trip
// tests run against; the table entries are trusted developer input, so there is
// no untrusted network parsing here.
class static_discovery final : public discovery
{
public:
    explicit static_discovery(std::vector<service_info> table)
        : m_table(std::move(table))
    {
    }

    void advertise(const service_info &service) override
    {
        m_table.push_back(service);
    }

    void browse(const resolved_callback &on_resolved) override
    {
        for(const auto &entry : m_table)
            on_resolved(entry);
    }

    void stop() override
    {
    }

private:
    std::vector<service_info> m_table;
};

}

#endif
