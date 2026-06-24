// over-limit: one cohesive topic-registry lifecycle matrix; the acquire/release/teardown cells
// share the one stub-broker + recording-notifier registry harness, and that shared fixture
// preamble alone exceeds the file ceiling, so the cells cannot split across TUs without
// scattering that one harness into over-budget shells.
#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <vector>

// The demand-driven topic registry (header-only, asio/strand/std::function-free): a
// stub broker over heap-backed regions + a recording stub notifier prove the
// acquire/release refcount lifecycle (created -> attached -> teardown+unlink), the
// max_payload ring sizing with a subscriber-only default fallback, and the
// teardown ordering (disarm BEFORE the subscriber the drain touches is destroyed).

using namespace plexus::shm;

namespace {

// A shared region store: named, page-region-shaped heap buffers that survive a
// handle's teardown (so an attach after a create maps the SAME bytes, and an
// unlink removes the name). A stand-in for /dev/shm in a single-process unit test.
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
    int                                            unlinks = 0;

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

// The handle the stub broker hands back: it pins the shared region alive and, for a
// creator, unlinks the name on release.
class stub_handle
{
public:
    stub_handle() = default;

    stub_handle(region_store *store, std::string name, std::shared_ptr<region_store::region> region,
                bool owns_name) noexcept
            : m_store(store)
            , m_name(std::move(name))
            , m_region(std::move(region))
            , m_owns(owns_name)
    {
    }

    ~stub_handle() { reclaim(); }

    stub_handle(const stub_handle &)            = delete;
    stub_handle &operator=(const stub_handle &) = delete;

    stub_handle(stub_handle &&o) noexcept
            : m_store(o.m_store)
            , m_name(std::move(o.m_name))
            , m_region(std::move(o.m_region))
            , m_owns(o.m_owns)
    {
        o.m_store = nullptr;
        o.m_owns  = false;
    }

    stub_handle &operator=(stub_handle &&o) noexcept
    {
        if(this != &o)
        {
            reclaim();
            m_store   = o.m_store;
            m_name    = std::move(o.m_name);
            m_region  = std::move(o.m_region);
            m_owns    = o.m_owns;
            o.m_store = nullptr;
            o.m_owns  = false;
        }
        return *this;
    }

    std::span<std::byte> bytes() const { return {m_region->base, m_region->size}; }

private:
    void reclaim() noexcept
    {
        if(m_owns && m_store != nullptr)
        {
            if(auto it = m_store->regions.find(m_name); it != m_store->regions.end())
                it->second->live = false;
            m_store->regions.erase(m_name);
            ++m_store->unlinks;
        }
        m_store = nullptr;
        m_owns  = false;
        m_region.reset();
    }

    region_store                         *m_store = nullptr;
    std::string                           m_name;
    std::shared_ptr<region_store::region> m_region;
    bool                                  m_owns = false;
};

// The stub broker satisfying the core region_broker concept over the store. create
// mints a fresh name (already_exists when live), attach maps a live one.
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
        if(auto it = m_store.regions.find(key); it != m_store.regions.end() && it->second->live)
            return region_status::already_exists;
        out = stub_handle(&m_store, key, m_store.make(key, bytes), /*owns=*/true);
        return region_status::ok;
    }

    region_status attach(std::string_view name, region_handle &out)
    {
        const std::string key{name};
        auto              it = m_store.regions.find(key);
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = stub_handle(&m_store, key, it->second, /*owns=*/false);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>) {}

private:
    region_store &m_store;
};

static_assert(region_broker<stub_broker>, "stub_broker must satisfy the region_broker seam");

// A file-scope order log the recording notifier writes to (no test-only hook on the
// production registry): disarm() logs "disarm", and the notifier DESTRUCTOR logs
// "notify_dtor". Because the registry calls disarm() in teardown() BEFORE it erases
// the entry, and the entry destructs the channel (its subscriber) BEFORE the
// notifier (declaration order ring, notify, channel -> reverse-destructs channel,
// notify), the sequence "disarm" then "notify_dtor" brackets the subscriber
// teardown: disarm is logged before any entry destructor runs, proving
// stop-before-subscriber-teardown.
std::vector<std::string> g_order;

struct recording_notifier
{
    bool armed = false;

    recording_notifier()                                      = default;
    recording_notifier(const recording_notifier &)            = delete;
    recording_notifier &operator=(const recording_notifier &) = delete;
    recording_notifier(recording_notifier &&)                 = default;
    recording_notifier &operator=(recording_notifier &&)      = default;

    ~recording_notifier()
    {
        if(armed)
            g_order.push_back("notify_dtor");
    }

    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) { armed = true; }
    void disarm() noexcept
    {
        if(armed)
            g_order.push_back("disarm");
    }
};

static_assert(notifier<recording_notifier>, "recording_notifier must satisfy the notifier seam");

using test_registry = shm_topic_registry<stub_broker, recording_notifier>;

}

TEST_CASE("shm.registry demand-drives the ring lifecycle by refcount", "[shm][registry]")
{
    region_store  store;
    stub_broker   creator_broker{store};
    test_registry registry(creator_broker, plexus::io::reliability::reliable,
                           plexus::io::congestion::block);

    const std::string fqn = "topic.demo";

    // First acquire mints the ring (the creator owns both regions: control + slab).
    REQUIRE(registry.acquire(fqn, ring_direction::request, 0) == acquire_result::created);
    REQUIRE(registry.live_count() == 1);
    REQUIRE(store.unlinks == 0);

    // A second acquire of the SAME key bumps the refcount (still created, one entry).
    REQUIRE(registry.acquire(fqn, ring_direction::request, 0) == acquire_result::created);
    REQUIRE(registry.live_count() == 1);

    // release 2 -> 1 keeps the ring (no unlink yet).
    registry.release(fqn, ring_direction::request);
    REQUIRE(registry.live_count() == 1);
    REQUIRE(store.unlinks == 0);

    // release 1 -> 0 tears it down AND unlinks both regions (the creator owns them).
    registry.release(fqn, ring_direction::request);
    REQUIRE(registry.live_count() == 0);
    REQUIRE(store.unlinks == 2); // control + slab
}

TEST_CASE("shm.registry a peer collision attaches and a subscriber-only acquire defaults geometry",
          "[shm][registry]")
{
    region_store  store;
    stub_broker   broker_a{store};
    stub_broker   broker_b{store};
    test_registry creator(broker_a, plexus::io::reliability::best_effort,
                          plexus::io::congestion::drop_newest);
    test_registry attacher(broker_b, plexus::io::reliability::best_effort,
                           plexus::io::congestion::drop_newest);

    const std::string fqn = "topic.shared";

    // The creator mints with an explicit max_payload (it sizes the ring).
    REQUIRE(creator.acquire(fqn, ring_direction::request, /*max_payload=*/256) ==
            acquire_result::created);

    // A second registry over the same store collides on create and ATTACHES to the
    // peer's live regions -- the subscriber-only acquire (max_payload == 0) falls
    // back to the default geometry, but it re-reads the creator's geometry off the
    // control header, so the two ends agree.
    REQUIRE(attacher.acquire(fqn, ring_direction::request, /*max_payload=*/0) ==
            acquire_result::attached);

    // A send over the creator's channel drains back over the attacher's channel: the
    // two registries share ONE ring through the deterministic name (round-trip proof
    // the geometry agreed).
    auto *send_ch = creator.channel_for(fqn, ring_direction::request);
    auto *recv_ch = attacher.channel_for(fqn, ring_direction::request);
    REQUIRE(send_ch != nullptr);
    REQUIRE(recv_ch != nullptr);

    const std::uint32_t value = 0x1234ABCDu;
    std::byte           payload[sizeof(value)];
    std::memcpy(payload, &value, sizeof(value));
    REQUIRE(send_ch->send(std::span<const std::byte>(payload, sizeof(payload))) == loan_status::ok);

    std::uint32_t             got     = 0;
    int                       count   = 0;
    test_registry::deliver_fn deliver = [&](plexus::wire_bytes<shm_slot_owner> wb)
    {
        std::memcpy(&got, wb.data(), sizeof(got));
        ++count;
    };
    recv_ch->drain(deliver);
    REQUIRE(count == 1);
    REQUIRE(got == value);

    // Tear down: the attacher releases first (no unlink -- it never owned the name),
    // then the creator (unlinks both).
    attacher.release(fqn, ring_direction::request);
    REQUIRE(store.unlinks == 0);
    creator.release(fqn, ring_direction::request);
    REQUIRE(store.unlinks == 2);
}

TEST_CASE("shm.registry disarms the notifier before the subscriber teardown", "[shm][registry]")
{
    region_store store;
    stub_broker  broker{store};
    g_order.clear();

    test_registry registry(broker, plexus::io::reliability::reliable,
                           plexus::io::congestion::block);
    REQUIRE(registry.acquire("topic.order", ring_direction::request, 0) == acquire_result::created);

    // The release tears the entry down: disarm() fires (logging "disarm") BEFORE the
    // entry is erased, and the entry destructs the channel (its subscriber) before
    // the notifier (which logs "notify_dtor" on the way out). So "disarm" preceding
    // "notify_dtor" proves the notifier was stopped before the subscriber the drain
    // touches was destroyed -- the non-negotiable teardown ordering.
    registry.release("topic.order", ring_direction::request);
    REQUIRE(registry.live_count() == 0);

    REQUIRE(g_order.size() == 2);
    REQUIRE(g_order[0] == "disarm");
    REQUIRE(g_order[1] == "notify_dtor");
}
