#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H

#include "launch_helpers.h"
#include "topology_block.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/region_naming.h"

#include <array>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>

#include <sys/mman.h>

namespace shm_harness {

inline std::string summary(const node_counts &c, bool over_shm)
{
    return "received=" + std::to_string(c.received) + " distinct=" + std::to_string(c.distinct) + " duplicates=" + std::to_string(c.duplicates) +
            " warm_seen=" + std::to_string(c.warm_seen) + " connected=" + std::to_string(c.connected) + " shm_region=" + (over_shm ? "1" : "0");
}

class topology
{
public:
    topology(std::string fqn, std::uint32_t count)
            : m_fqn(std::move(fqn))
            , m_count(count)
            , m_ports(free_tcp_ports(count))
            , m_name("shm_harness.")
            , m_block(plexus::testing::xproc_shared<topology_block>::create(m_name.str()))
            , m_nodes()
    {
    }

    topology(const topology &)            = delete;
    topology &operator=(const topology &) = delete;

    ~topology()
    {
        for(std::optional<plexus::testing::xproc_process> &node : m_nodes)
            node.reset();
        for(const std::string &region : {region_name(), region_name() + ".s"})
            ::shm_unlink(("/plexus." + region).c_str());
    }

    bool created() const
    {
        return m_block.has_value() && m_count <= k_max_nodes;
    }

    bool spawn_node(std::uint32_t index, node_role role, bool hint, bool deny)
    {
        slot(index).ready.store(0, std::memory_order_release);
        std::vector<std::string> args{m_name.str(), std::to_string(index), std::to_string(m_count), m_fqn, std::string(role_name(role)), hint ? "1" : "0", deny ? "1" : "0"};
        for(const std::uint16_t port : m_ports)
            args.push_back(std::to_string(port));

        m_nodes[index] = plexus::testing::xproc_process::spawn("shm_harness.node", args);
        if(!m_nodes[index])
            return false;
        return wait_until([&] { return slot(index).ready.load(std::memory_order_acquire) != 0 || !m_nodes[index]->running(); }, 10s) &&
                slot(index).ready.load(std::memory_order_acquire) != 0;
    }

    // An accepted session is keyed by a provisional id, so a node observes only the sessions it dialed: node i is
    // fully connected once it reaches every lower-indexed node.
    bool wait_connected(std::chrono::milliseconds bound)
    {
        return wait_until(
                [&]
                {
                    for(std::uint32_t index = 0; index < m_count; ++index)
                    {
                        if(slot(index).connected.load(std::memory_order_acquire) != index)
                            return false;
                    }
                    return true;
                },
                bound);
    }

    bool command(std::uint32_t index, node_command cmd, std::uint32_t arg = 0, std::chrono::milliseconds bound = 10s)
    {
        const std::uint32_t seq = issue(index, cmd, arg);
        return wait_until([&] { return slot(index).ack_seq.load(std::memory_order_acquire) == seq; }, bound);
    }

    bool warm_up(const std::vector<std::uint32_t> &publishers, const std::vector<std::uint32_t> &subscribers, std::chrono::milliseconds bound)
    {
        bool commanded = true;
        for(const std::uint32_t index : publishers)
            commanded = command(index, node_command::warm) && commanded;
        const bool seen = commanded && wait_until([&] { return every(subscribers, [](const node_counts &c) { return c.warm_seen > 0; }); }, bound);
        for(const std::uint32_t index : publishers)
            command(index, node_command::none);
        return seen;
    }

    // Counted traffic still in flight after the target is reached would surface as duplicates in the settle window.
    bool publish_round(std::uint32_t publisher, std::uint32_t count, const std::vector<std::uint32_t> &subscribers, std::chrono::milliseconds bound)
    {
        for(const std::uint32_t index : subscribers)
            command(index, node_command::reset);
        command(publisher, node_command::publish, count);
        const bool reached = wait_until([&] { return every(subscribers, [count](const node_counts &c) { return c.received >= count; }); }, bound);
        std::this_thread::sleep_for(500ms);
        return reached;
    }

    bool leave(std::uint32_t index, std::chrono::milliseconds bound = 10s)
    {
        issue(index, node_command::leave, 0);
        const bool exited = m_nodes[index] && m_nodes[index]->wait_for(bound) == std::optional<std::int32_t>(0);
        m_nodes[index].reset();
        return exited;
    }

    node_counts counts(std::uint32_t index)
    {
        return snapshot(slot(index));
    }

    bool region_attaches() const
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        return broker.attach(region_name(), region) == plexus::shm::region_status::ok;
    }

private:
    std::string m_fqn;
    std::uint32_t m_count;
    std::vector<std::uint16_t> m_ports;
    plexus::testing::scoped_shm_name m_name;
    std::optional<plexus::testing::xproc_shared<topology_block>> m_block;
    std::array<std::optional<plexus::testing::xproc_process>, k_max_nodes> m_nodes;

    node_slot &slot(std::uint32_t index)
    {
        return m_block->get().nodes[index];
    }

    std::string region_name() const
    {
        return plexus::shm::region_name_for(m_fqn, plexus::shm::ring_direction::request);
    }

    std::uint32_t issue(std::uint32_t index, node_command cmd, std::uint32_t arg)
    {
        slot(index).command.store(static_cast<std::uint32_t>(cmd), std::memory_order_release);
        slot(index).arg.store(arg, std::memory_order_release);
        return slot(index).command_seq.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    template<typename Pred>
    bool every(const std::vector<std::uint32_t> &indices, Pred pred)
    {
        for(const std::uint32_t index : indices)
        {
            if(!pred(counts(index)))
                return false;
        }
        return true;
    }
};

}

#endif
