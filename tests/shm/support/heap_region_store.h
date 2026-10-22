#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HEAP_REGION_STORE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HEAP_REGION_STORE_H

#include "plexus/shm/ring_layout.h"

#include <map>
#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::testing {

// An in-memory region, cache-line aligned as a mapping would be, that outlives the handles mapping it.
struct heap_region
{
    explicit heap_region(std::size_t region_bytes)
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

// Named heap regions, the single-process stand-in for /dev/shm: an attach after a create maps the same bytes.
struct heap_region_store
{
    using region = heap_region;

    std::map<std::string, std::shared_ptr<heap_region>> regions;

    std::shared_ptr<heap_region> make(const std::string &name, std::size_t bytes)
    {
        auto r        = std::make_shared<heap_region>(bytes);
        regions[name] = r;
        return r;
    }
};

}

#endif
