#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPIC_REGIONS_H

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/region_naming.h"

#include <string>
#include <cstdint>
#include <filesystem>
#include <system_error>

#include <sys/mman.h>

namespace shm_harness {

// On destruction it unlinks the topic's request ring, the control region and its ".s" slab, which a killed process
// leaves behind.
class topic_regions
{
public:
    explicit topic_regions(std::string fqn)
            : m_region(plexus::shm::region_name_for(fqn, plexus::shm::ring_direction::request))
    {
    }

    topic_regions(const topic_regions &)            = delete;
    topic_regions &operator=(const topic_regions &) = delete;

    ~topic_regions()
    {
        for(const std::string &region : {m_region, m_region + ".s"})
            ::shm_unlink(("/plexus." + region).c_str());
    }

    bool attaches() const
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        return broker.attach(m_region, region) == plexus::shm::region_status::ok;
    }

    // Linux backs POSIX shared memory with /dev/shm, so the topic's live names are the entries there.
    std::uint32_t entries() const
    {
        std::uint32_t count = 0;
        std::error_code ec;
        for(const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator("/dev/shm", ec))
        {
            if(entry.path().filename().string().starts_with("plexus." + m_region))
                ++count;
        }
        return count;
    }

private:
    std::string m_region;
};

}

#endif
