#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_REGION_DOORBELLS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_REGION_DOORBELLS_H

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/doorbell_naming.h"

#include <atomic>
#include <string>
#include <cstdint>
#include <utility>
#include <filesystem>
#include <string_view>
#include <system_error>

#include <semaphore.h>

namespace shm_harness {

inline std::string sem_name_for(std::string_view region, std::uint32_t slot, std::uint32_t tag)
{
    return std::string(plexus::shm::format_doorbell_name(plexus::shm::k_semaphore_doorbell_prefix, region, slot, tag).view());
}

// On destruction it unlinks the semaphore doorbells a killed waiter of the region leaves behind: the names the region's
// cursors still tag, and on Linux, where named semaphores are /dev/shm entries, every doorbell entry of the region.
class region_doorbells
{
public:
    explicit region_doorbells(std::string region)
            : m_region(std::move(region))
    {
    }

    region_doorbells(const region_doorbells &)            = delete;
    region_doorbells &operator=(const region_doorbells &) = delete;

    ~region_doorbells()
    {
        unlink_tagged();
#if defined(__linux__)
        unlink_listed();
#endif
    }

private:
    std::string m_region;

    void unlink_tagged() const
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        if(broker.attach(m_region, region) != plexus::shm::region_status::ok || region.bytes().size() < sizeof(plexus::shm::control_header_t))
            return;
        const auto &header = *reinterpret_cast<const plexus::shm::control_header_t *>(region.bytes().data());
        for(std::uint32_t slot = 0; slot < plexus::shm::k_max_consumers; ++slot)
            ::sem_unlink(sem_name_for(m_region, slot, header.cursors[slot].waiter_tag.load(std::memory_order_acquire)).c_str());
    }

    void unlink_listed() const
    {
        const std::string entry_prefix = "sem." + std::string(plexus::shm::k_semaphore_doorbell_prefix.substr(1)) + m_region;
        std::error_code ec;
        for(const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator("/dev/shm", ec))
        {
            const std::string file = entry.path().filename().string();
            if(file.starts_with(entry_prefix))
                ::sem_unlink(("/" + file.substr(4)).c_str());
        }
    }
};

}

#endif
