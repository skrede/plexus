#include "support/noop_notifier.h"
#include "support/always_live_slots.h"
#include "support/heap_region_store.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

// ring_memory_for is the cost a consumer queries BEFORE it declares a ring, so it must equal two things
// byte-for-byte: the layout helpers' sum for the geometry ring_geometry_for yields, and the bytes the registry
// actually requests from the broker at mint. The depth-32 16 MiB figure (512 MiB) is pinned so a regression
// back to the old depth-17 (or a deep-tier off-by-one) is caught.

using namespace plexus::shm;
using plexus::io::ring_geometry_mode;

namespace {

constexpr std::size_t k_kib = 1024;
constexpr std::size_t k_mib = 1024 * 1024;

// A recording broker over heap-backed regions that captures the byte size of every create request, so a test can
// compare ring_memory_for against what the registry actually asks the allocator for. The control region and the slab
// region are both recorded; the slab is the one ring_memory_for's slab term must match.
struct recording_store : plexus::testing::heap_region_store
{
    std::vector<std::size_t> slab_requests;
    std::size_t last_total = 0;
};

class recording_handle : public plexus::testing::always_live_slots
{
public:
    recording_handle() = default;

    recording_handle(recording_store *store, std::string name, std::shared_ptr<recording_store::region> region) noexcept
            : m_store(store)
            , m_name(std::move(name))
            , m_region(std::move(region))
    {
    }

    ~recording_handle()
    {
        reclaim();
    }

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

    std::span<std::byte> bytes() const
    {
        return m_region->bytes();
    }

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

    recording_store *m_store = nullptr;
    std::string m_name;
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

    region_status create(std::string_view name, std::size_t bytes, const create_options &, region_handle &out)
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
        auto it = m_store.regions.find(key);
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = recording_handle(&m_store, key, it->second);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>)
    {
    }

private:
    recording_store &m_store;
};

static_assert(region_broker<recording_broker>, "recording_broker must satisfy the region_broker seam");

using silent_notifier = plexus::testing::noop_notifier;

static_assert(notifier<silent_notifier>, "silent_notifier must satisfy the notifier seam");

using test_registry = shm_topic_registry<recording_broker, silent_notifier>;

}

TEST_CASE("shm.ring_memory_query ring_memory_for equals the layout helpers for a (P,C) table", "[shm][ring_memory_query]")
{
    for(std::uint32_t payload : {static_cast<std::uint32_t>(512 * k_kib), static_cast<std::uint32_t>(k_mib)})
        for(std::uint32_t capacity : {1u, 2u, 16u})
        {
            const ring_geometry g      = ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, capacity);
            const std::size_t expected = control_region_bytes(g.cell_count) + slab_region_bytes(g.cell_count, g.slot_capacity);
            REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, capacity) == expected);
        }
}

TEST_CASE("shm.ring_memory_query the 16 MiB reliable_preserving cost at C=16 is the depth-32 figure", "[shm][ring_memory_query]")
{
    // A 16 MiB payload at the full 16-consumer capacity is a depth-32 ring (never depth-17), whose 512 MiB slot area the
    // default ceiling admits.
    const std::uint32_t payload = static_cast<std::uint32_t>(16 * k_mib);
    const ring_geometry g       = ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, 16u);
    REQUIRE(g.cell_count == 32u);
    REQUIRE(g.slot_capacity == 16 * k_mib);
    REQUIRE(slab_payload_bytes(g.cell_count, g.slot_capacity) == static_cast<std::size_t>(512) * k_mib);

    acquire_failure failure{};
    CHECK(detail::topic_within_ceiling(g, k_max_ring_slab_bytes, failure));
    CHECK(failure.ask_bytes == 0);
    const std::size_t expected = control_region_bytes(g.cell_count) + slab_region_bytes(g.cell_count, g.slot_capacity);
    REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, 16u) == expected);
}

TEST_CASE("shm.ring_memory_query ring_memory_for matches the bytes the registry actually requests", "[shm][ring_memory_query]")
{
    // The query and the registry's mint must agree: the slab the registry asks the
    // broker to create equals ring_memory_for's slab term for the same (P, mode, C).
    recording_store store;
    recording_broker broker{store};
    test_registry registry(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);

    const std::uint32_t payload     = static_cast<std::uint32_t>(512 * k_kib);
    const std::uint32_t capacity    = 2u;
    const ring_geometry g           = ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, capacity);
    const std::size_t expected_slab = slab_region_bytes(g.cell_count, g.slot_capacity);

    REQUIRE(registry.acquire("topic.query", ring_direction::request, {}, ring_role::producer, payload, ring_geometry_mode::reliable_preserving, capacity) == acquire_result::created);

    REQUIRE(store.slab_requests.size() == 1);
    REQUIRE(store.slab_requests.front() == expected_slab);
    // And the query a consumer would have run beforehand matches that slab term.
    REQUIRE(ring_memory_for(payload, ring_geometry_mode::reliable_preserving, capacity) == control_region_bytes(g.cell_count) + expected_slab);

    registry.release("topic.query", ring_direction::request, {}, ring_role::producer);
}
