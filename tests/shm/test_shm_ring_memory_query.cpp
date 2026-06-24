// over-limit: one cohesive ring-memory-cost query matrix; the cells share the one recording
// stub-broker registry harness that captures the requested slab sizes, so splitting them
// scatters that shared recording-broker fixture into near-empty per-cell shells.
#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <vector>

// ring_memory_for is the cost a consumer queries BEFORE it declares a ring, so it must
// equal two things byte-for-byte: the layout helpers' sum for the geometry
// ring_geometry_for yields, and the bytes the registry actually requests from the
// broker at mint. The depth-32 16 MiB figure (512 MiB) is pinned so a regression back to
// the old depth-17 (or a deep-tier off-by-one) is caught.

using namespace plexus::shm;

namespace {

constexpr std::size_t k_kib = 1024;
constexpr std::size_t k_mib = 1024 * 1024;

// A recording broker over heap-backed regions that captures the byte size of every
// create request, so a test can compare ring_memory_for against what the registry
// actually asks the allocator for. The control region and the slab region are both
// recorded; the slab is the one ring_memory_for's slab term must match.
struct recording_store
{
    struct region
    {
        std::vector<std::byte> storage;
        std::byte             *base = nullptr;
        std::size_t            size = 0;
        bool                   live = false;
    };

    std::map<std::string, std::shared_ptr<region>> regions;
    std::vector<std::size_t>                       slab_requests;
    std::size_t                                    last_total = 0;

    std::shared_ptr<region> make(const std::string &name, std::size_t bytes)
    {
        auto r = std::make_shared<region>();
        r->storage.assign(bytes + k_cache_line, std::byte{});
        auto raw      = reinterpret_cast<std::uintptr_t>(r->storage.data());
        auto algn     = (raw + k_cache_line - 1) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        r->base       = reinterpret_cast<std::byte *>(algn);
        r->size       = bytes;
        r->live       = true;
        regions[name] = r;
        return r;
    }
};

class recording_handle
{
public:
    recording_handle() = default;

    recording_handle(recording_store *store, std::string name,
                     std::shared_ptr<recording_store::region> region) noexcept
            : m_store(store)
            , m_name(std::move(name))
            , m_region(std::move(region))
    {
    }

    ~recording_handle() { reclaim(); }

    recording_handle(const recording_handle &)            = delete;
    recording_handle &operator=(const recording_handle &) = delete;

    recording_handle(recording_handle &&o) noexcept
            : m_store(o.m_store)
            , m_name(std::move(o.m_name))
            , m_region(std::move(o.m_region))
    {
        o.m_store = nullptr;
    }

    recording_handle &operator=(recording_handle &&o) noexcept
    {
        if(this != &o)
        {
            reclaim();
            m_store   = o.m_store;
            m_name    = std::move(o.m_name);
            m_region  = std::move(o.m_region);
            o.m_store = nullptr;
        }
        return *this;
    }

    std::span<std::byte> bytes() const { return {m_region->base, m_region->size}; }

private:
    void reclaim() noexcept
    {
        if(m_store != nullptr)
        {
            if(auto it = m_store->regions.find(m_name); it != m_store->regions.end())
                it->second->live = false;
            m_store->regions.erase(m_name);
        }
        m_store = nullptr;
        m_region.reset();
    }

    recording_store                         *m_store = nullptr;
    std::string                              m_name;
    std::shared_ptr<recording_store::region> m_region;
};

class recording_broker
{
public:
    using region_handle = recording_handle;

    explicit recording_broker(recording_store &store) noexcept
            : m_store(store)
    {
    }

    region_status create(std::string_view name, std::size_t bytes, const create_options &,
                         region_handle &out)
    {
        const std::string key{name};
        if(key.size() >= 2 && key.compare(key.size() - 2, 2, ".s") == 0)
            m_store.slab_requests.push_back(bytes);
        if(auto it = m_store.regions.find(key); it != m_store.regions.end() && it->second->live)
            return region_status::already_exists;
        out = recording_handle(&m_store, key, m_store.make(key, bytes));
        return region_status::ok;
    }

    region_status attach(std::string_view name, region_handle &out)
    {
        const std::string key{name};
        auto              it = m_store.regions.find(key);
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = recording_handle(&m_store, key, it->second);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>) {}

private:
    recording_store &m_store;
};

static_assert(region_broker<recording_broker>,
              "recording_broker must satisfy the region_broker seam");

struct silent_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(notifier<silent_notifier>, "silent_notifier must satisfy the notifier seam");

using test_registry = shm_topic_registry<recording_broker, silent_notifier>;

}

TEST_CASE("shm.ring_memory_query ring_memory_for equals the layout helpers for a (P,C) table",
          "[shm][ring_memory_query]")
{
    for(std::uint32_t payload :
        {static_cast<std::uint32_t>(512 * k_kib), static_cast<std::uint32_t>(k_mib)})
        for(std::uint32_t capacity : {1u, 2u, 16u})
        {
            const ring_geometry g =
                    ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, capacity);
            const std::size_t expected = control_region_bytes(g.cell_count) +
                    slab_region_bytes(g.cell_count, g.slot_capacity);
            REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, capacity) ==
                    expected);
        }
}

TEST_CASE(
        "shm.ring_memory_query the 16 MiB reliable_preserving cost at C=16 is the depth-32 figure",
        "[shm][ring_memory_query]")
{
    // The expensive corner: a 16 MiB payload at the full 16-consumer capacity is a
    // depth-32 ring (next power of two STRICTLY above 16, never depth-17). slab = 32 *
    // round_up_8(16 MiB) = 512 MiB; control = 1344 + 32*64 = 3392 bytes. This pins the
    // corrected math so a future depth-17 (or deep-tier) regression is caught.
    const std::uint32_t payload = static_cast<std::uint32_t>(16 * k_mib);
    const ring_geometry g =
            ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, 16u);
    REQUIRE(g.cell_count == 32u);
    REQUIRE(g.slot_capacity == 16 * k_mib);

    const std::size_t slab = slab_region_bytes(g.cell_count, g.slot_capacity);
    REQUIRE(slab == static_cast<std::size_t>(512) * k_mib);

    const std::size_t expected = control_region_bytes(g.cell_count) + slab; // 512 MiB + 3392 bytes
    REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, 16u) == expected);
}

TEST_CASE("shm.ring_memory_query ring_memory_for matches the bytes the registry actually requests",
          "[shm][ring_memory_query]")
{
    // The query and the registry's mint must agree: the slab the registry asks the
    // broker to create equals ring_memory_for's slab term for the same (P, mode, C).
    recording_store  store;
    recording_broker broker{store};
    test_registry    registry(broker, plexus::io::reliability::reliable,
                              plexus::io::congestion::block);

    const std::uint32_t payload  = static_cast<std::uint32_t>(512 * k_kib);
    const std::uint32_t capacity = 2u;
    const ring_geometry g =
            ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, capacity);
    const std::size_t expected_slab = slab_region_bytes(g.cell_count, g.slot_capacity);

    REQUIRE(registry.acquire("topic.query", ring_direction::request, payload,
                             ring_geometry_mode::reliable_preserving,
                             capacity) == acquire_result::created);

    REQUIRE(store.slab_requests.size() == 1);
    REQUIRE(store.slab_requests.front() == expected_slab);
    // And the query a consumer would have run beforehand matches that slab term.
    REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, capacity) ==
            control_region_bytes(g.cell_count) + expected_slab);

    registry.release("topic.query", ring_direction::request);
}
