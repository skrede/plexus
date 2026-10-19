#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_DELAYING_BROKER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_DELAYING_BROKER_H

#include "always_live_slots.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <map>
#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::testing {

// An in-memory region that outlives the handles mapping it; its creator's handle clears live on release.
struct delaying_region
{
    explicit delaying_region(std::size_t region_bytes)
            : live(true)
            , size(region_bytes)
            , storage(region_bytes + ::plexus::shm::k_cache_line, std::byte{})
    {
    }

    std::span<std::byte> bytes() noexcept
    {
        const auto raw     = reinterpret_cast<std::uintptr_t>(storage.data());
        const auto aligned = (raw + ::plexus::shm::k_cache_line - 1) & ~static_cast<std::uintptr_t>(::plexus::shm::k_cache_line - 1);
        return {storage.data() + (aligned - raw), size};
    }

    bool live;
    std::size_t size;
    std::vector<std::byte> storage;
};

class delaying_handle : public always_live_slots
{
public:
    delaying_handle() noexcept
            : m_owns(false)
    {
    }

    delaying_handle(std::shared_ptr<delaying_region> region, bool owns) noexcept
            : m_owns(owns)
            , m_region(std::move(region))
    {
    }

    delaying_handle(delaying_handle &&o) noexcept
            : m_owns(std::exchange(o.m_owns, false))
            , m_region(std::move(o.m_region))
    {
    }

    delaying_handle &operator=(delaying_handle &&o) noexcept
    {
        if(this != &o)
        {
            release();
            m_owns   = std::exchange(o.m_owns, false);
            m_region = std::move(o.m_region);
        }
        return *this;
    }

    ~delaying_handle()
    {
        release();
    }

    std::span<std::byte> bytes() const noexcept
    {
        return m_region->bytes();
    }

private:
    bool m_owns;
    std::shared_ptr<delaying_region> m_region;

    void release() noexcept
    {
        if(m_owns && m_region)
            m_region->live = false;
        m_owns = false;
        m_region.reset();
    }
};

// A region broker over in-memory regions that can withhold a region from attach until a deadline, the way a co-host
// creator still minting its ring leaves the control region visible before the slab exists.
class delaying_broker
{
public:
    using clock         = std::chrono::steady_clock;
    using region_handle = delaying_handle;

    ::plexus::shm::region_status create(std::string_view name, std::size_t bytes, const ::plexus::shm::create_options &, region_handle &out)
    {
        const std::string key{name};
        if(auto it = m_regions.find(key); it != m_regions.end() && it->second->live)
            return ::plexus::shm::region_status::already_exists;
        auto region = std::make_shared<delaying_region>(bytes);
        m_regions.insert_or_assign(key, region);
        out = delaying_handle{std::move(region), true};
        return ::plexus::shm::region_status::ok;
    }

    ::plexus::shm::region_status attach(std::string_view name, region_handle &out)
    {
        const std::string key{name};
        auto it = m_regions.find(key);
        if(it == m_regions.end() || !it->second->live || withheld(key))
            return ::plexus::shm::region_status::not_found;
        out = delaying_handle{it->second, false};
        return ::plexus::shm::region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>)
    {
    }

    void withhold_until(std::string name, clock::time_point visible_at)
    {
        m_withheld.insert_or_assign(std::move(name), visible_at);
    }

private:
    std::map<std::string, clock::time_point> m_withheld;
    std::map<std::string, std::shared_ptr<delaying_region>> m_regions;

    bool withheld(const std::string &name) const
    {
        auto it = m_withheld.find(name);
        return it != m_withheld.end() && clock::now() < it->second;
    }
};

static_assert(::plexus::shm::region_broker<delaying_broker>, "delaying_broker must satisfy the region_broker seam");

}

#endif
