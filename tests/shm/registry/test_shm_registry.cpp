#include "support/always_live_slots.h"
#include "support/heap_region_store.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>

// The demand-driven topic registry (header-only, asio/strand/std::function-free): a stub broker over heap-backed
// regions + a recording stub notifier prove the acquire/release refcount lifecycle (created -> attached -> teardown,
// no release removing a name), the max_payload ring sizing with a subscriber-only default fallback, and the teardown
// ordering (disarm BEFORE the subscriber the drain touches is destroyed).

using namespace plexus::shm;

namespace {

// The heap region store, counting the names an unlink removed.
struct region_store : plexus::testing::heap_region_store
{
    std::int32_t unlinks = 0;
};

// The handle the stub broker hands back: it pins the shared region alive, and only unlink_name removes the name.
class stub_handle : public plexus::testing::always_live_slots
{
public:
    stub_handle() = default;

    stub_handle(region_store *store, std::string name, std::shared_ptr<region_store::region> region) noexcept
            : m_store(store)
            , m_name(std::move(name))
            , m_region(std::move(region))
    {
    }

    ~stub_handle()
    {
        reclaim();
    }

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

    std::span<std::byte> bytes() const
    {
        return m_region->bytes();
    }

    void unlink_name() noexcept
    {
        if(m_store == nullptr || m_store->regions.erase(m_name) == 0)
            return;
        ++m_store->unlinks;
    }

private:
    void reclaim() noexcept
    {
        m_store = nullptr;
        m_region.reset();
    }

    region_store *m_store = nullptr;
    std::string m_name;
    std::shared_ptr<region_store::region> m_region;
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

    region_status create(std::string_view name, std::size_t bytes, const create_options &, region_handle &out)
    {
        const std::string key{name};
        if(auto it = m_store.regions.find(key); it != m_store.regions.end() && it->second->live)
            return region_status::already_exists;
        out = stub_handle(&m_store, key, m_store.make(key, bytes));
        return region_status::ok;
    }

    region_status attach(std::string_view name, region_handle &out)
    {
        const std::string key{name};
        auto it = m_store.regions.find(key);
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = stub_handle(&m_store, key, it->second);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>)
    {
    }

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

    ~recording_notifier()
    {
        if(armed)
            g_order.push_back("notify_dtor");
    }

    void signal() noexcept
    {
    }
    arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        armed = true;
        return arm_status::armed;
    }
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
    region_store store;
    stub_broker creator_broker{store};
    test_registry registry(creator_broker, plexus::io::reliability::reliable, plexus::io::congestion::block);

    const std::string fqn = "topic.demo";

    // First acquire mints the ring (the creator owns both regions: control + slab).
    REQUIRE(registry.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer, 0) == acquire_result::created);
    REQUIRE(registry.live_count() == 1);
    REQUIRE(store.unlinks == 0);

    // A second acquire of the SAME key bumps the refcount (still created, one entry).
    REQUIRE(registry.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer, 0) == acquire_result::created);
    REQUIRE(registry.live_count() == 1);

    // release 2 -> 1 keeps the ring (no unlink yet).
    registry.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer);
    REQUIRE(registry.live_count() == 1);
    REQUIRE(store.unlinks == 0);

    // release 1 -> 0 tears it down; the stub's slots always read held elsewhere, so no last holder removes a name.
    registry.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer);
    REQUIRE(registry.live_count() == 0);
    REQUIRE(store.unlinks == 0);
}

TEST_CASE("shm.registry a peer collision attaches and a consumer reads the creator's geometry", "[shm][registry]")
{
    region_store store;
    stub_broker broker_a{store};
    stub_broker broker_b{store};
    test_registry creator(broker_a, plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest);
    test_registry attacher(broker_b, plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest);

    const std::string fqn = "topic.shared";

    // The creator mints with an explicit max_payload (it sizes the ring).
    REQUIRE(creator.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer, /*max_payload=*/256) == acquire_result::created);

    // A second registry over the same store ATTACHES to the peer's live regions as a
    // consumer, which never sizes a ring: it re-reads the creator's geometry off the
    // control header (the max_payload it passes is ignored), so the two ends agree
    // whatever the consumer would have asked for.
    REQUIRE(attacher.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::consumer, /*max_payload=*/0) == acquire_result::attached);

    // A send over the creator's channel drains back over the attacher's channel: the
    // two registries share ONE ring through the deterministic name (round-trip proof
    // the geometry agreed).
    auto *send_ch = creator.channel_for(fqn, ring_direction::request, plexus::node_id{});
    auto *recv_ch = attacher.channel_for(fqn, ring_direction::request, plexus::node_id{});
    REQUIRE(send_ch != nullptr);
    REQUIRE(recv_ch != nullptr);

    const std::uint32_t value = 0x1234ABCDu;
    std::byte payload[sizeof(value)];
    std::memcpy(payload, &value, sizeof(value));
    REQUIRE(send_ch->send(std::span<const std::byte>(payload, sizeof(payload))) == loan_status::ok);

    std::uint32_t got                 = 0;
    int count                         = 0;
    test_registry::deliver_fn deliver = [&](plexus::wire_bytes<shm_slot_owner> wb)
    {
        std::memcpy(&got, wb.data(), sizeof(got));
        ++count;
    };
    recv_ch->drain(deliver);
    REQUIRE(count == 1);
    REQUIRE(got == value);

    // Tear down in either order: no release removes a name.
    attacher.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::consumer);
    REQUIRE(store.unlinks == 0);
    creator.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::producer);
    REQUIRE(store.unlinks == 0);
}

TEST_CASE("shm.registry disarms the notifier before the subscriber teardown", "[shm][registry]")
{
    region_store store;
    stub_broker broker{store};
    g_order.clear();

    test_registry registry(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    REQUIRE(registry.acquire("topic.order", ring_direction::request, plexus::node_id{}, ring_role::both, 0) == acquire_result::created);

    // The release tears the entry down: disarm() fires (logging "disarm") BEFORE the
    // entry is erased, and the entry destructs the channel (its subscriber) before
    // the notifier (which logs "notify_dtor" on the way out). So "disarm" preceding
    // "notify_dtor" proves the notifier was stopped before the subscriber the drain
    // touches was destroyed -- the non-negotiable teardown ordering.
    registry.release("topic.order", ring_direction::request, plexus::node_id{}, ring_role::both);
    REQUIRE(registry.live_count() == 0);

    REQUIRE(g_order.size() == 2);
    REQUIRE(g_order[0] == "disarm");
    REQUIRE(g_order[1] == "notify_dtor");
}
