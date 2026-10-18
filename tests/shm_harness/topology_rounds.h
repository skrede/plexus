#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_ROUNDS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_ROUNDS_H

#include "launch_helpers.h"
#include "topology_block.h"

#include <chrono>
#include <thread>
#include <vector>
#include <cstdint>

namespace shm_harness {

template<typename Topology>
class topology_rounds
{
public:
    bool warm_up(const std::vector<std::uint32_t> &publishers, const std::vector<std::uint32_t> &subscribers, std::chrono::milliseconds bound)
    {
        bool commanded     = true;
        std::uint32_t mask = 0;
        for(const std::uint32_t index : publishers)
        {
            commanded = self().command(index, node_command::warm) && commanded;
            mask |= 1u << index;
        }
        const bool seen = commanded && wait_until([&] { return every(subscribers, [mask](const node_counts &c) { return (c.warm_seen & mask) == mask; }); }, bound);
        for(const std::uint32_t index : publishers)
            self().command(index, node_command::none);
        return seen;
    }

    // Counted traffic still in flight after the target is reached would surface as duplicates in the settle window.
    bool publish_round(const std::vector<std::uint32_t> &publishers, std::uint32_t count, const std::vector<std::uint32_t> &subscribers, std::chrono::milliseconds bound)
    {
        for(const std::uint32_t index : subscribers)
            self().command(index, node_command::reset);
        for(const std::uint32_t index : publishers)
            self().command(index, node_command::publish, count, bound);
        const std::uint32_t target = count * static_cast<std::uint32_t>(publishers.size());
        const bool reached         = wait_until([&] { return every(subscribers, [target](const node_counts &c) { return c.received >= target; }); }, bound);
        std::this_thread::sleep_for(500ms);
        return reached;
    }

private:
    Topology &self()
    {
        return static_cast<Topology &>(*this);
    }

    template<typename Pred>
    bool every(const std::vector<std::uint32_t> &indices, Pred pred)
    {
        for(const std::uint32_t index : indices)
        {
            if(!pred(self().counts(index)))
                return false;
        }
        return true;
    }
};

}

#endif
