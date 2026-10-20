#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_LAUNCH_H

#include "node_config.h"
#include "topic_regions.h"
#include "launch_helpers.h"
#include "topology_block.h"
#include "topology_rounds.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace shm_harness {

class topology : public topology_rounds<topology>
{
public:
    topology(std::string fqn, std::uint32_t count, dial_wiring wiring = dial_wiring::one_way)
            : m_fqn(std::move(fqn))
            , m_count(count)
            , m_wiring(wiring)
            , m_ports(free_tcp_ports(count))
            , m_name("shm_harness.")
            , m_regions(m_fqn, harness_ids(count))
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

    // A later warm_up then waits for publishers this node has already seen, such as a restarted one.
    void forget_warm(std::uint32_t index)
    {
        slot(index).warm_seen.store(0, std::memory_order_release);
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
};

}

#endif
