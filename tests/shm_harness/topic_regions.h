#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H

#include "support/region_names.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/node_id.h"

#include <string>
#include <vector>
#include <cstdint>
#include <filesystem>
#include <system_error>

#include <sys/mman.h>

namespace shm_harness {

// On destruction it unlinks each publisher's request ring for the topic, the control region and its ".s" slab, which a
// killed process leaves behind.
class topic_regions
{
public:
    topic_regions(const std::string &fqn, const std::vector<plexus::node_id> &publishers)
            : m_regions()
    {
        for(const plexus::node_id &publisher : publishers)
            m_regions.push_back(plexus::testing::region_name(fqn, publisher));
    }

    topic_regions(const topic_regions &)            = delete;
    topic_regions &operator=(const topic_regions &) = delete;

    ~topic_regions()
    {
        for(const std::string &region : m_regions)
        {
            ::shm_unlink(("/plexus." + region).c_str());
            ::shm_unlink(("/plexus." + region + ".s").c_str());
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

private:
    std::vector<std::string> m_regions;
};

}

#endif
