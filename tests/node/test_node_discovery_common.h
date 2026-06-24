// The node discovery-wiring oracle, over the live static_discovery: advertise-at-birth,
// listen() port-key append + live re-advertise, browse-to-awareness into router().known(),
// and the malformed-card reject table (untrusted multicast input).
#pragma once

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>

namespace node_discovery_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::service_info;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
}

inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect   = forever_cfg();
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

struct host
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};
    static_discovery disc{{}};
};

// A browser that records every card it is notified of, for asserting advertise-at-birth
// and the listen() live update.
struct recording_browser
{
    std::vector<service_info> seen;

    void attach(static_discovery &disc)
    {
        disc.browse([this](const service_info &s) { seen.push_back(s); });
    }

    const service_info *latest_for(const std::string &name) const
    {
        const service_info *found = nullptr;
        for(const auto &s : seen)
            if(s.name == name)
                found = &s;
        return found;
    }
};

inline bool has_key(const service_info &s, std::string_view key)
{
    return std::any_of(s.metadata.begin(), s.metadata.end(), [&](const auto &kv) { return kv.first == key; });
}

inline bool has_port_key(const service_info &s)
{
    return std::any_of(s.metadata.begin(), s.metadata.end(),
                       [](const auto &kv) { return kv.first.rfind("plexus/", 0) == 0 && kv.first.size() > 5 && kv.first.substr(kv.first.size() - 5) == "/port"; });
}

} // namespace node_discovery_fixture
