// over-limit: one cohesive safe-defaults + fail-closed geometry matrix; the cells share the one
// heap-backed stub-broker registry harness, so splitting them scatters that shared broker
// fixture into near-empty per-cell shells.
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/region_broker_concept.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/shm_mux_member.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/shm_topic_registry.h"

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

// Two guarantees. (1) The safe-defaults regression: an unset/small declaration keeps the
// byte-identical historical default ring (cell_count 256, slot 4096), so raising the
// slab ceiling never perturbs the small-payload path. (2) The unified fail-closed path:
// a registration over the tunable ceiling, AND one the broker/OS cannot map, BOTH return
// acquire_result::failed and name WHICH bound was hit (ceiling vs OS allocator) with the
// exact ask vs available — never a silent reliable -> best-effort downgrade.

using namespace plexus::io::shm;

namespace {

constexpr std::size_t k_mib = 1024 * 1024;

// A heap-backed region store + broker (the create/attach seam) shared by the
// fail-closed legs. The OS-allocation-failed leg flips fail_slab_create so the slab
// create returns region_status::failed, modeling an allocator that cannot map the
// region the ceiling admitted.
struct region_store
{
    struct region
    {
        std::vector<std::byte> storage;
        std::byte             *base = nullptr;
        std::size_t            size = 0;
        bool                   live = false;
    };

    std::map<std::string, std::shared_ptr<region>> regions;
    bool                                           fail_slab_create = false;

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

class stub_handle
{
public:
    stub_handle() = default;

    stub_handle(region_store *store, std::string name,
                std::shared_ptr<region_store::region> region) noexcept
            : m_store(store)
            , m_name(std::move(name))
            , m_region(std::move(region))
    {
    }

    ~stub_handle() { reclaim(); }

    stub_handle(const stub_handle &)            = delete;
    stub_handle &operator=(const stub_handle &) = delete;

    stub_handle(stub_handle &&o) noexcept
            : m_store(o.m_store)
            , m_name(std::move(o.m_name))
            , m_region(std::move(o.m_region))
    {
        o.m_store = nullptr;
    }

    stub_handle &operator=(stub_handle &&o) noexcept
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

    region_store                         *m_store = nullptr;
    std::string                           m_name;
    std::shared_ptr<region_store::region> m_region;
};

class stub_broker
{
public:
    using region_handle = stub_handle;

    explicit stub_broker(region_store &store) noexcept
            : m_store(store)
    {
    }

    region_status create(std::string_view name, std::size_t bytes, const create_options &,
                         region_handle &out)
    {
        const std::string key{name};
        const bool        is_slab = key.size() >= 2 && key.compare(key.size() - 2, 2, ".s") == 0;
        if(is_slab && m_store.fail_slab_create)
            return region_status::failed;
        if(auto it = m_store.regions.find(key); it != m_store.regions.end() && it->second->live)
            return region_status::already_exists;
        out = stub_handle(&m_store, key, m_store.make(key, bytes));
        return region_status::ok;
    }

    region_status attach(std::string_view name, region_handle &out)
    {
        const std::string key{name};
        auto              it = m_store.regions.find(key);
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = stub_handle(&m_store, key, it->second);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>) {}

private:
    region_store &m_store;
};

static_assert(region_broker<stub_broker>, "stub_broker must satisfy the region_broker seam");

struct silent_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(notifier<silent_notifier>, "silent_notifier must satisfy the notifier seam");

using test_registry = shm_topic_registry<stub_broker, silent_notifier>;
using test_member   = shm_mux_member<stub_broker, silent_notifier>;

}

TEST_CASE("shm.geometry_defaults the small-payload default geometry is byte-identical",
          "[shm][geometry_defaults]")
{
    // The historical default ring: an unset declaration keeps cell_count 256, slot 4096.
    const ring_geometry unset = ring_geometry_for(std::nullopt);
    REQUIRE(unset.cell_count == 256u);
    REQUIRE(unset.slot_capacity == 4096u);

    // And the explicit reliable_preserving / shipped-floor form agrees byte-for-byte.
    const ring_geometry explicit_default = ring_geometry_for(
            std::nullopt, ring_geometry_mode::reliable_preserving, k_max_consumers);
    REQUIRE(explicit_default.cell_count == 256u);
    REQUIRE(explicit_default.slot_capacity == 4096u);

    // A small (<=4096) declared payload still lands in the default deep-but-narrow ring.
    const ring_geometry small =
            ring_geometry_for(std::optional<std::uint32_t>{4096u},
                              ring_geometry_mode::reliable_preserving, k_max_consumers);
    REQUIRE(small.cell_count == 256u);
    REQUIRE(small.slot_capacity == 4096u);

    // The same-host ring defaults relocated from node_options onto the shm transport ship the
    // BYTE-IDENTICAL values the node now sources from the member: the {0, reliable_preserving}
    // geometry (max_consumers 0 resolves to the capacity floor above) and the consumer-sovereign
    // auto-upgrade policy. This is the relocation's behavioral-identity proof (no field migration,
    // no value drift) — the unprovisioned default resolves through default_geometry() to the SAME
    // 256/4096 ring asserted above.
    region_store store;
    stub_broker  broker{store};
    test_member  member{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};

    const shm_geometry def = member.default_geometry();
    REQUIRE(def.max_consumers == 0u);
    REQUIRE(def.mode == ring_geometry_mode::reliable_preserving);

    // The default policy reproduces attempt_shm_upgrade across the full (same_host x hint) matrix:
    // it engages only when same-host AND a qualifying hint is set, and can only decline otherwise.
    for(const bool same_host : {false, true})
        for(const dispatch_hint hint :
            {dispatch_hint::none, dispatch_hint::frequent, dispatch_hint::large,
             dispatch_hint::priority, dispatch_hint::frequent | dispatch_hint::large})
            REQUIRE(member.upgrade_policy()(same_host, hint) ==
                    attempt_shm_upgrade(same_host, hint));
}

TEST_CASE(
        "shm.geometry_defaults an over-ceiling registration fails closed naming the ceiling bound",
        "[shm][geometry_defaults]")
{
    region_store store;
    stub_broker  broker{store};

    // A deliberately-small tunable ceiling: small enough that even a modest reliable ring
    // exceeds it, so the registration must fail closed against the CEILING bound rather
    // than mint a downgraded or shrunk ring.
    constexpr std::uint64_t tiny_ceiling = 64 * 1024; // 64 KiB
    test_registry registry(broker, plexus::io::reliability::reliable, plexus::io::congestion::block,
                           test_registry::default_notifier_binder(), tiny_ceiling);

    const std::uint32_t payload = static_cast<std::uint32_t>(k_mib); // 1 MiB ring slab >> 64 KiB
    REQUIRE(registry.acquire("topic.over_ceiling", ring_direction::request, payload,
                             ring_geometry_mode::reliable_preserving,
                             1u) == acquire_result::failed);

    // The diagnostic names the CEILING bound with the exact ask vs the ceiling, and the
    // ring was never minted (no live entry, no silent downgrade).
    const acquire_failure &f = registry.last_acquire_failure();
    REQUIRE(f.bound == acquire_bound::slab_ceiling);
    REQUIRE(f.limit_bytes == tiny_ceiling);
    REQUIRE(f.ask_bytes > tiny_ceiling);
    REQUIRE(registry.live_count() == 0);
}

TEST_CASE("shm.geometry_defaults an OS-allocation-failed registration fails closed naming the OS "
          "bound",
          "[shm][geometry_defaults]")
{
    region_store store;
    store.fail_slab_create = true; // the broker/OS cannot map the slab the ceiling admitted
    stub_broker broker{store};

    // A generous (default) ceiling: the ceiling leg passes, so the failure can ONLY be the
    // OS-allocator leg — the second fail-closed path.
    test_registry registry(broker, plexus::io::reliability::reliable,
                           plexus::io::congestion::block);

    const std::uint32_t payload = static_cast<std::uint32_t>(512 * 1024);
    const ring_geometry g = ring_geometry_for(payload, ring_geometry_mode::reliable_preserving, 1u);
    const std::size_t   expected_ask = slab_region_bytes(g.cell_count, g.slot_capacity);

    REQUIRE(registry.acquire("topic.os_fail", ring_direction::request, payload,
                             ring_geometry_mode::reliable_preserving,
                             1u) == acquire_result::failed);

    // The diagnostic names the OS-ALLOCATOR bound, carries the exact slab ask and the
    // broker's verdict, and the ring was not minted (no silent best-effort path).
    const acquire_failure &f = registry.last_acquire_failure();
    REQUIRE(f.bound == acquire_bound::os_allocator);
    REQUIRE(f.ask_bytes == expected_ask);
    REQUIRE(f.broker == region_status::failed);
    REQUIRE(registry.live_count() == 0);
}
