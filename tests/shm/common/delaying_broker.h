#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_DELAYING_BROKER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_DELAYING_BROKER_H

#include "always_live_slots.h"
#include "heap_region_store.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <map>
#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus::testing {

// Its creator's handle clears the region's live flag on release.
class delaying_handle : public always_live_slots
{
public:
    delaying_handle() noexcept
            : m_owns(false)
    {
    }

    delaying_handle(std::shared_ptr<heap_region> region, bool owns) noexcept
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
    std::shared_ptr<heap_region> m_region;

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
        auto region = std::make_shared<heap_region>(bytes);
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

    // The handles mapping a region of this broker now; an empty or moved-from handle maps none.
    std::size_t mapped() const
    {
        std::size_t count = 0;
        for(const auto &[name, region] : m_regions)
            count += static_cast<std::size_t>(region.use_count() - 1);
        return count;
    }

private:
    std::map<std::string, clock::time_point> m_withheld;
    std::map<std::string, std::shared_ptr<heap_region>> m_regions;

    bool withheld(const std::string &name) const
    {
        auto it = m_withheld.find(name);
        return it != m_withheld.end() && clock::now() < it->second;
    }
};

static_assert(::plexus::shm::region_broker<delaying_broker>, "delaying_broker must satisfy the region_broker seam");

}

#endif
