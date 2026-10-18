#ifndef HPP_GUARD_PLEXUS_SHM_SHM_TOPIC_REGISTRY_H
#define HPP_GUARD_PLEXUS_SHM_SHM_TOPIC_REGISTRY_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/shm/detail/shm_topic_open.h"
#include "plexus/shm/detail/shm_topic_sink.h"
#include "plexus/shm/detail/shm_topic_entry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/discovery/universe.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::shm {

// The demand-driven lifecycle owner of every live (topic, direction, publisher) ring: a first acquire MINTS the ring (or
// ATTACHES to a peer's), each further acquire bumps a refcount, and the matching releases tear it down at 1 -> 0 (the
// creator also unlinks). A waiter lost after its acquire tells each holder watching it and fails further acquires of
// its key. It borrows the broker by reference and owns one notifier per entry. Templated on the broker + notifier
// seams so core pulls no POSIX/asio header.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_topic_registry
{
public:
    using broker_type = Broker;
    using deliver_fn  = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // A notifier that wakes on a cross-process futex is NOT default-constructible (it binds to
    // the ring's control header + the user's executor), so the binder injects that construction
    // without coupling the registry to a concrete notifier ctor.
    using notifier_binder = plexus::detail::move_only_function<void(std::optional<Notifier> &, control_header_t &, std::string_view)>;

    static notifier_binder default_notifier_binder() noexcept
    {
        return [](std::optional<Notifier> &slot, control_header_t &, std::string_view) { slot.emplace(); };
    }

    // max_ring_slab_bytes defaults to the shipped k_max_ring_slab_bytes; the acquire fails closed above it.
    shm_topic_registry(Broker &broker, io::reliability rel, io::congestion cong, notifier_binder bind_notifier = default_notifier_binder(),
                       std::uint64_t max_ring_slab_bytes = k_max_ring_slab_bytes, std::string region_ns = {}, std::uint32_t universe = discovery::k_default_universe) noexcept
            : m_broker(broker)
            , m_reliability(rel)
            , m_congestion(cong)
            , m_universe(universe)
            , m_bind_notifier(std::move(bind_notifier))
            , m_max_ring_slab_bytes(max_ring_slab_bytes)
            , m_region_ns(std::move(region_ns))
            , m_last_failure()
            , m_entries()
    {
    }

    shm_topic_registry(const shm_topic_registry &)            = delete;
    shm_topic_registry &operator=(const shm_topic_registry &) = delete;
    shm_topic_registry(shm_topic_registry &&)                 = delete;
    shm_topic_registry &operator=(shm_topic_registry &&)      = delete;

    ~shm_topic_registry()
    {
        for(auto &[k, e] : m_entries)
            e->teardown();
    }

    // A repeat acquire of a live key bumps the refcount and returns its prior verdict; a first acquire mints (or attaches
    // to a peer's) ring (max_payload 0 -> default). Each publisher of a topic owns its own ring.
    acquire_result acquire(const std::string &fqn, ring_direction direction, const node_id &publisher, std::uint32_t max_payload,
                           ring_geometry_mode mode = ring_geometry_mode::reliable_preserving, std::uint32_t consumer_capacity = 0, acquire_mode amode = acquire_mode::reclaim_stale)
    {
        const key k{fqn, direction, publisher};
        if(auto it = m_entries.find(k); it != m_entries.end())
            return detail::topic_reacquire(*it->second, m_last_failure);

        auto e                       = std::make_unique<entry>();
        m_last_failure               = acquire_failure{};
        const acquire_result verdict = detail::topic_open_ring(*this, *e, k, max_payload, mode, consumer_capacity, amode);
        if(verdict == acquire_result::failed)
            return acquire_result::failed;
        // A waiter whose notifier cannot arm is never woken, so the acquire fails and the caller keeps its other transports.
        if(const arm_status armed = detail::topic_bind(*this, *e, k, verdict); armed != arm_status::armed)
        {
            e->teardown();
            m_last_failure = acquire_failure{acquire_bound::notifier, 0, 0, region_status::ok, armed};
            return acquire_result::failed;
        }
        m_entries.emplace(k, std::move(e));
        return verdict;
    }

    void release(const std::string &fqn, ring_direction direction, const node_id &publisher)
    {
        auto it = m_entries.find(key{fqn, direction, publisher});
        if(it == m_entries.end())
            return;
        if(--it->second->refcount > 0)
            return;
        it->second->teardown();
        m_entries.erase(it);
    }

    void set_consumer_sink(const std::string &fqn, ring_direction direction, const node_id &publisher, deliver_fn sink)
    {
        detail::topic_set_sink(m_entries, key{fqn, direction, publisher}, std::move(sink));
    }

    // Called on the receive companion's teardown BEFORE its ring release (disarm-before-destroy).
    void clear_consumer_sink(const std::string &fqn, ring_direction direction, const node_id &publisher)
    {
        detail::topic_clear_sink(m_entries, key{fqn, direction, publisher});
    }

    shm_channel<Notifier> *channel_for(const std::string &fqn, ring_direction direction, const node_id &publisher)
    {
        return detail::topic_channel(m_entries, key{fqn, direction, publisher});
    }

    loss_watch watch_loss(const std::string &fqn, ring_direction direction, const node_id &publisher, waiter_loss::lost_fn lost)
    {
        return detail::topic_watch_loss(m_entries, key{fqn, direction, publisher}, std::move(lost));
    }

    std::size_t live_count() const noexcept
    {
        return m_entries.size();
    }

    // bound == none after a successful acquire.
    const acquire_failure &last_acquire_failure() const noexcept
    {
        return m_last_failure;
    }

    // Bounds the slab of every ring minted AFTER it is set.
    void set_max_ring_slab_bytes(std::uint64_t bytes) noexcept
    {
        m_max_ring_slab_bytes = bytes;
    }

    // Names every ring minted or attached AFTER it is set, so rings of different universes never meet.
    void bind_universe(std::uint32_t universe) noexcept
    {
        m_universe = universe;
    }

    std::uint32_t universe() const noexcept
    {
        return m_universe;
    }

private:
    using key   = detail::shm_topic_key;
    using entry = detail::shm_topic_entry<Broker, Notifier>;

    Broker &m_broker;
    io::reliability m_reliability;
    io::congestion m_congestion;
    std::uint32_t m_universe;
    notifier_binder m_bind_notifier;
    std::uint64_t m_max_ring_slab_bytes;
    std::string m_region_ns;
    acquire_failure m_last_failure;
    detail::shm_topic_map<entry> m_entries;

    template<typename R, typename E>
    friend ::plexus::shm::acquire_result detail::topic_open_ring(R &, E &, const detail::shm_topic_key &, std::uint32_t, ::plexus::shm::ring_geometry_mode, std::uint32_t,
                                                                 ::plexus::shm::acquire_mode);
    template<typename R, typename E>
    friend ::plexus::shm::arm_status detail::topic_bind(R &, E &, const detail::shm_topic_key &, ::plexus::shm::acquire_result);
};

}

#endif
