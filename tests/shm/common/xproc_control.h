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
// anonymous mapping is inherited (Windows). The block is never destroyed in place; unmapping releases it, and the
// creator removes its name as it goes.
template<typename Block>
class xproc_shared
{
    static_assert(std::is_standard_layout_v<Block> && std::is_trivially_destructible_v<Block>);
    // A lock-based std::atomic keeps its lock in process-local memory, so only always-lock-free atomics synchronize
    // across processes.
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free && std::atomic<std::uint32_t>::is_always_lock_free);

public:
    xproc_shared(xproc_shared &&other) noexcept
            : m_created(std::exchange(other.m_created, false))
            , m_region(std::move(other.m_region))
    {
    }

    xproc_shared(const xproc_shared &)            = delete;
    xproc_shared &operator=(const xproc_shared &) = delete;
    xproc_shared &operator=(xproc_shared &&)      = delete;

    ~xproc_shared()
    {
        if(m_created)
            m_region.unlink_name();
    }

    static std::optional<xproc_shared> create(std::string_view name)
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        broker.unlink(name);
        if(broker.create(name, sizeof(Block), plexus::shm::create_options{}, region) != plexus::shm::region_status::ok)
            return std::nullopt;
        ::new(static_cast<void *>(region.bytes().data())) Block{};
        return xproc_shared(std::move(region), true);
    }

    static std::optional<xproc_shared> attach(std::string_view name)
    {
        plexus::native::shm_region_broker broker;
        plexus::native::shm_region_handle region;
        if(broker.attach(name, region) != plexus::shm::region_status::ok || region.bytes().size() < sizeof(Block))
            return std::nullopt;
        return xproc_shared(std::move(region), false);
    }

    Block &get()
    {
        return *std::launder(reinterpret_cast<Block *>(m_region.bytes().data()));
    }

private:
    xproc_shared(plexus::native::shm_region_handle region, bool created)
            : m_created(created)
            , m_region(std::move(region))
    {
    }

    bool m_created;
    plexus::native::shm_region_handle m_region;
};

}

#endif
