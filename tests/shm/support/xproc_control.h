#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_CONTROL_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_CONTROL_H

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/region_broker_concept.h"

#include <new>
#include <atomic>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <type_traits>

namespace plexus::testing {

// A named block of atomics the parent creates and re-exec'd children attach by name, so it works where no
// anonymous mapping is inherited (Windows). The block is never destroyed in place; unmapping releases it.
template<typename Block>
class xproc_shared
{
    static_assert(std::is_standard_layout_v<Block> && std::is_trivially_destructible_v<Block>);
    // A lock-based std::atomic keeps its lock in process-local memory, so only always-lock-free atomics synchronize
    // across processes.
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free && std::atomic<std::uint32_t>::is_always_lock_free);

public:
    static std::optional<xproc_shared> create(std::string_view name)
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        const plexus::shm::create_options options{.unlink_stale_on_create = true, .perms = 0600};
        if(broker.create(name, sizeof(Block), options, region) != plexus::shm::region_status::ok)
            return std::nullopt;
        ::new(static_cast<void *>(region.bytes().data())) Block{};
        return xproc_shared(std::move(region));
    }

    static std::optional<xproc_shared> attach(std::string_view name)
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        if(broker.attach(name, region) != plexus::shm::region_status::ok || region.bytes().size() < sizeof(Block))
            return std::nullopt;
        return xproc_shared(std::move(region));
    }

    Block &get()
    {
        return *std::launder(reinterpret_cast<Block *>(m_region.bytes().data()));
    }

private:
    explicit xproc_shared(plexus::native::shm_region_handle region)
            : m_region(std::move(region))
    {
    }

    plexus::native::shm_region_handle m_region;
};

}

#endif
