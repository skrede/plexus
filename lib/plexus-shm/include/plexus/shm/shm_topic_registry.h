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

#include "plexus/shm/detail/shm_topic_sink.h"
#include "plexus/shm/detail/shm_topic_entry.h"
#include "plexus/shm/detail/shm_topic_acquire.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/same_host_geometry.h"

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

// The demand-driven lifecycle owner of every live (topic, direction, publisher) ring: a first acquire MINTS the ring,
// ADOPTS this publisher's ring a dead creator left, or ATTACHES to a peer's; each acquire counts one holder of its role,
// and the entry tears down once no role holds it (removing the ring's names when no holder is left anywhere). A waiter
// lost after its acquire tells each holder watching it and fails further acquires of its key. Templated on the broker
// and notifier seams so core pulls no POSIX or asio header.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_topic_registry
{
public:
    using broker_type = Broker;
    using deliver_fn  = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // A notifier that wakes on a cross-process futex is NOT default-constructible (it binds to the ring's control header
    // and the user's executor), so the binder injects that construction without coupling the registry to its ctor.
    using notifier_binder = plexus::detail::move_only_function<void(std::optional<Notifier> &, control_header_t &, std::string_view)>;

    static notifier_binder default_notifier_binder() noexcept
    {
        return [](std::optional<Notifier> &slot, control_header_t &, std::string_view) { slot.emplace(); };
    }

    shm_topic_registry(Broker &broker, io::reliability rel, io::congestion cong, notifier_binder bind_notifier = default_notifier_binder(),
                       std::uint64_t max_ring_slab_bytes = k_max_ring_slab_bytes, std::string region_ns = {}, std::uint32_t universe = discovery::k_default_universe) noexcept
            : m_broker(broker)
            , m_reliability(rel)
            , m_congestion(cong)
            , m_universe(universe)
            , m_bind_notifier(std::move(bind_notifier))
            , m_fallback_cb()
            , m_max_ring_slab_bytes(max_ring_slab_bytes)
            , m_region_ns(std::move(region_ns))
            , m_last_failure()
            , m_entries()
    {
    }

    shm_topic_registry(const shm_topic_registry &)            = delete;
    shm_topic_registry &operator=(const shm_topic_registry &) = delete;

    ~shm_topic_registry()
    {
        for(auto &[k, e] : m_entries)
            detail::ring_lifetime::close(m_broker, *e);
    }

    // role is the holder's use of the ring: only a consuming role takes a cursor and arms, and it never creates the ring
    // (none under the name is not_found; the geometry arguments size only a ring a producer mints, max_payload 0 ->
    // default). A repeat acquire of a live key returns its first verdict. Each publisher of a topic owns its own ring,
    // and a second live producer of it is refused as creator_alive.
    acquire_result acquire(const std::string &fqn, ring_direction direction, const node_id &publisher, ring_role role, std::uint32_t max_payload,
                           io::ring_geometry_mode mode = io::ring_geometry_mode::reliable_preserving, std::uint32_t consumer_capacity = 0)
    {
        return detail::topic_acquire(*this, key{fqn, direction, publisher}, role, max_payload, mode, consumer_capacity);
    }

    void release(const std::string &fqn, ring_direction direction, const node_id &publisher, ring_role role)
    {
        detail::topic_release(*this, key{fqn, direction, publisher}, role);
    }

    // Told once per lost waiter and once per notifier demotion of any live entry, on the executor.
    void on_fallback(plexus::detail::move_only_function<void(io::fallback_reason)> report)
    {
        m_fallback_cb = std::move(report);
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
    plexus::detail::move_only_function<void(io::fallback_reason)> m_fallback_cb;
    std::uint64_t m_max_ring_slab_bytes;
    std::string m_region_ns;
    acquire_failure m_last_failure;
    detail::shm_topic_map<entry> m_entries;

    friend struct detail::ring_adoption;
    template<typename R>
    friend ::plexus::shm::acquire_result detail::topic_acquire(R &, const detail::shm_topic_key &, ::plexus::shm::ring_role, std::uint32_t, ::plexus::io::ring_geometry_mode,
                                                               std::uint32_t);
    template<typename R>
    friend void detail::topic_release(R &, const detail::shm_topic_key &, ::plexus::shm::ring_role);
    template<typename R, typename E>
    friend ::plexus::shm::acquire_result detail::topic_open_ring(R &, E &, const detail::shm_topic_key &, ::plexus::shm::ring_role, std::uint32_t, ::plexus::io::ring_geometry_mode,
                                                                 std::uint32_t);
    template<typename R, typename E>
    friend ::plexus::shm::acquire_failure detail::topic_bind(R &, E &, ::plexus::shm::ring_role, ::plexus::shm::acquire_result);
};

}

#endif
