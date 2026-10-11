#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H

#include "topic_regions.h"
#include "launch_helpers.h"
#include "topology_block.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include <array>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace shm_harness {

class topology
{
public:
    topology(std::string fqn, std::uint32_t count, dial_wiring wiring = dial_wiring::one_way)
            : m_fqn(std::move(fqn))
            , m_count(count)
            , m_wiring(wiring)
            , m_ports(free_tcp_ports(count))
            , m_name("shm_harness.")
            , m_regions(m_fqn)
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
    }

    bool created() const
    {
        return m_block.has_value() && m_count <= k_max_nodes;
    }

    bool spawn_node(std::uint32_t index, node_role role, bool hint, bool deny, node_composition composition = node_composition::same_host)
    {
        const bool crossed = m_wiring == dial_wiring::crossed;
        std::vector<std::string> args{m_name.str(),     std::to_string(index), std::to_string(m_count), m_fqn, std::string(role_name(role)),
                                      hint ? "1" : "0", deny ? "1" : "0",      crossed ? "1" : "0"};
        args.emplace_back(composition_name(composition));
        for(const std::uint16_t port : m_ports)
            args.push_back(std::to_string(port));
        return launch(index, "shm_harness.node", args);
    }

    bool spawn_registry(std::uint32_t index, node_role role)
    {
        return launch(index, "shm_harness.registry", {m_name.str(), std::to_string(index), m_fqn, std::string(role_name(role))});
    }

    void kill(std::uint32_t index)
    {
        if(m_nodes[index])
            m_nodes[index]->kill();
        m_nodes[index].reset();
    }

    // An accepted session is keyed by a provisional id, so a node observes only the sessions it dialed: one way, node i
    // is fully connected once it reaches every lower-indexed node; crossed, once it reaches every other node.
    bool wait_connected(std::chrono::milliseconds bound)
    {
        return wait_until(
                [&]
                {
                    for(std::uint32_t index = 0; index < m_count; ++index)
                    {
                        const std::uint32_t dialed = m_wiring == dial_wiring::crossed ? m_count - 1 : index;
                        if(slot(index).connected.load(std::memory_order_acquire) != dialed)
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
        bool commanded     = true;
        std::uint32_t mask = 0;
        for(const std::uint32_t index : publishers)
        {
            commanded = command(index, node_command::warm) && commanded;
            mask |= 1u << index;
        }
        const bool seen = commanded && wait_until([&] { return every(subscribers, [mask](const node_counts &c) { return (c.warm_seen & mask) == mask; }); }, bound);
        for(const std::uint32_t index : publishers)
            command(index, node_command::none);
        return seen;
    }

    // Counted traffic still in flight after the target is reached would surface as duplicates in the settle window.
    bool publish_round(const std::vector<std::uint32_t> &publishers, std::uint32_t count, const std::vector<std::uint32_t> &subscribers, std::chrono::milliseconds bound)
    {
        for(const std::uint32_t index : subscribers)
            command(index, node_command::reset);
        for(const std::uint32_t index : publishers)
            command(index, node_command::publish, count, bound);
        const std::uint32_t target = count * static_cast<std::uint32_t>(publishers.size());
        const bool reached         = wait_until([&] { return every(subscribers, [target](const node_counts &c) { return c.received >= target; }); }, bound);
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

    const topic_regions &regions() const
    {
        return m_regions;
    }

private:
    std::string m_fqn;
    std::uint32_t m_count;
    dial_wiring m_wiring;
    std::vector<std::uint16_t> m_ports;
    plexus::testing::scoped_shm_name m_name;
    topic_regions m_regions;
    std::optional<plexus::testing::xproc_shared<topology_block>> m_block;
    std::array<std::optional<plexus::testing::xproc_process>, k_max_nodes> m_nodes;

    node_slot &slot(std::uint32_t index)
    {
        return m_block->get().nodes[index];
    }

    bool launch(std::uint32_t index, std::string_view key, const std::vector<std::string> &args)
    {
        slot(index).ready.store(0, std::memory_order_release);
        m_nodes[index] = plexus::testing::xproc_process::spawn(key, args);
        if(!m_nodes[index])
            return false;
        return wait_until([&] { return slot(index).ready.load(std::memory_order_acquire) != 0 || !m_nodes[index]->running(); }, 10s) &&
                slot(index).ready.load(std::memory_order_acquire) != 0;
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
