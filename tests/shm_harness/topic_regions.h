#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H

#include "region_names.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/node_id.h"

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <filesystem>
#include <system_error>

namespace shm_harness {

struct ring_census
{
    std::uint64_t cells;
    std::uint32_t active_cursors;
};

// On destruction it unlinks each publisher's request ring for the topic, the control region and its ".s" slab, which a
// killed process leaves behind.
class topic_regions
{
public:
    topic_regions(const std::string &fqn, const std::vector<plexus::node_id> &publishers)
            : m_regions()
            , m_publishers(publishers)
    {
        for(const plexus::node_id &publisher : publishers)
            m_regions.push_back(plexus::testing::region_name(fqn, publisher));
    }

    topic_regions(const topic_regions &)            = delete;
    topic_regions &operator=(const topic_regions &) = delete;

    ~topic_regions()
    {
        plexus::native::shm_region_broker broker;
        for(const std::string &region : m_regions)
        {
            broker.unlink(region);
            broker.unlink(region + ".s");
        }
    }

    // Whether any publisher's ring is mapped.
    bool attaches() const
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle handle;
        for(const std::string &region : m_regions)
            if(broker.attach(region, handle) == plexus::shm::region_status::ok)
                return true;
        return false;
    }

    // Linux backs POSIX shared memory with /dev/shm, so the topic's live names are the entries there.
    std::uint32_t entries() const
    {
        std::uint32_t count = 0;
        std::error_code ec;
        for(const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator("/dev/shm", ec))
        {
            for(const std::string &region : m_regions)
                count += entry.path().filename().string().starts_with("plexus." + region) ? 1u : 0u;
        }
        return count;
    }

    // Publisher index's ring as a reader process maps it; nullopt when it is not mapped.
    std::optional<ring_census> census(std::size_t index) const
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle control;
        plexus::native::shm_region_handle slab;
        plexus::shm::broadcast_ring ring;
        if(broker.attach(m_regions[index], control) != plexus::shm::region_status::ok || broker.attach(m_regions[index] + ".s", slab) != plexus::shm::region_status::ok ||
           plexus::shm::broadcast_ring::attach(control.bytes(), slab.bytes(), m_publishers[index], ring) != plexus::shm::loan_status::ok)
            return std::nullopt;
        const std::uint64_t cursors = std::min<std::uint64_t>(ring.header().consumer_capacity, plexus::shm::k_max_consumers);
        std::uint32_t active        = 0;
        for(std::uint64_t i = 0; i < cursors; ++i)
            active += ring.header().cursors[i].active.load(std::memory_order_acquire) != 0 ? 1u : 0u;
        return ring_census{ring.cell_count(), active};
    }

private:
    std::vector<std::string> m_regions;
    std::vector<plexus::node_id> m_publishers;
};

}

#endif
