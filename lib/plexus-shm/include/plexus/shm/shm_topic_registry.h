#ifndef HPP_GUARD_PLEXUS_SHM_SHM_TOPIC_REGISTRY_H
#define HPP_GUARD_PLEXUS_SHM_SHM_TOPIC_REGISTRY_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/detail/shm_topic_open.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>

namespace plexus::shm {

// The demand-driven lifecycle owner of every live (topic, direction) ring: a first acquire
// MINTS the ring (or ATTACHES to a peer's), each further acquire bumps a refcount, and the
// matching releases tear it down at 1 -> 0 (the creator also unlinks). It borrows the broker
// by reference and owns one notifier per entry. Templated on the broker + notifier seams so
// core pulls no POSIX/asio header.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_topic_registry
{
public:
    using broker_type = Broker;
    using deliver_fn  = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // A notifier that wakes on a cross-process futex is NOT default-constructible (it binds to
    // the word + the user's executor), so the binder injects that construction without coupling
    // the registry to a concrete notifier ctor.
    using notifier_binder = plexus::detail::move_only_function<void(std::optional<Notifier> &, std::atomic<std::uint32_t> &, std::atomic<std::uint32_t> &)>;

    static notifier_binder default_notifier_binder() noexcept
    {
        return [](std::optional<Notifier> &slot, std::atomic<std::uint32_t> &, std::atomic<std::uint32_t> &) { slot.emplace(); };
    }

    // max_ring_slab_bytes defaults to the shipped k_max_ring_slab_bytes; the acquire fails
    // closed above it.
    shm_topic_registry(Broker &broker, io::reliability rel, io::congestion cong, notifier_binder bind_notifier = default_notifier_binder(),
                       std::uint64_t max_ring_slab_bytes = k_max_ring_slab_bytes, std::string region_ns = {}) noexcept
            : m_broker(broker)
            , m_reliability(rel)
            , m_congestion(cong)
            , m_bind_notifier(std::move(bind_notifier))
            , m_max_ring_slab_bytes(max_ring_slab_bytes)
            , m_region_ns(std::move(region_ns))
    {
    }

    shm_topic_registry(const shm_topic_registry &)            = delete;
    shm_topic_registry &operator=(const shm_topic_registry &) = delete;
    shm_topic_registry(shm_topic_registry &&)                 = delete;
    shm_topic_registry &operator=(shm_topic_registry &&)      = delete;

    ~shm_topic_registry()
    {
        teardown_all();
    }

    // A repeat acquire of a live key bumps the refcount and returns its prior verdict; a first
    // acquire mints (or attaches to a peer's) ring (max_payload 0 -> default).
    // NOLINTNEXTLINE(readability-function-size)
    acquire_result acquire(const std::string &fqn, ring_direction direction, std::uint32_t max_payload, ring_geometry_mode mode = ring_geometry_mode::reliable_preserving,
                           std::uint32_t consumer_capacity = 0, acquire_mode amode = acquire_mode::reclaim_stale)
    {
        const key k{fqn, direction};
        if(auto it = m_entries.find(k); it != m_entries.end())
        {
            ++it->second->refcount;
            return it->second->verdict;
        }

        auto e                       = std::make_unique<entry>();
        m_last_failure               = acquire_failure{};
        const acquire_result verdict = detail::topic_open_ring(*this, *e, fqn, direction, max_payload, mode, consumer_capacity, amode);
        if(verdict == acquire_result::failed)
            return acquire_result::failed;

        // The channel must be built AFTER the ring is bound (its subscriber registers a cursor
        // over the live ring).
        m_bind_notifier(e->notify, e->ring.notify_generation(), e->ring.park_state());
        e->channel.emplace(e->ring, *e->notify, m_reliability, m_congestion);
        e->verdict  = verdict;
        e->refcount = 1;
        entry *raw  = e.get();
        // The ONE drain authority for this ring's single consumer cursor: routing both delivery
        // and discard through the one armed drain keeps the cursor un-raced (a second drainer
        // would steal messages).
        e->notify->arm([raw] { raw->deliver_pending(); });
        m_entries.emplace(k, std::move(e));
        return verdict;
    }

    void release(const std::string &fqn, ring_direction direction)
    {
        const key k{fqn, direction};
        auto it = m_entries.find(k);
        if(it == m_entries.end())
            return;
        if(--it->second->refcount > 0)
            return;
        teardown(*it->second);
        m_entries.erase(it);
    }

    // The entry's wake-drain hands each pending message here instead of discarding; any
    // already-queued message is delivered NOW so a late-attaching consumer misses nothing.
    void set_consumer_sink(const std::string &fqn, ring_direction direction, deliver_fn sink)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it == m_entries.end())
            return;
        it->second->sink = std::move(sink);
        it->second->deliver_pending();
    }

    // Called on the receive companion's teardown BEFORE its ring release (disarm-before-destroy).
    void clear_consumer_sink(const std::string &fqn, ring_direction direction)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it != m_entries.end())
            it->second->sink = {};
    }

    shm_channel<Notifier> *channel_for(const std::string &fqn, ring_direction direction)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it == m_entries.end() || !it->second->channel)
            return nullptr;
        return &*it->second->channel;
    }

    void drain_channels(deliver_fn &deliver)
    {
        for(auto &[k, e] : m_entries)
            if(e->channel)
                e->channel->drain(deliver);
    }

    void drain()
    {
        deliver_fn discard = [](::plexus::wire_bytes<shm_slot_owner>) {};
        drain_channels(discard);
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

private:
    struct key
    {
        std::string fqn;
        ring_direction direction;

        bool operator==(const key &o) const noexcept
        {
            return direction == o.direction && fqn == o.fqn;
        }
    };

    struct key_hash
    {
        std::size_t operator()(const key &k) const noexcept
        {
            return std::hash<std::string>{}(k.fqn) ^ static_cast<std::size_t>(k.direction) * 0x9e3779b9u;
        }
    };

    // One live ring's owned state, pinned by the entry's unique_ptr so the non-movable
    // ring/channel/notifier hold stable addresses. The channel must destruct BEFORE the notifier
    // so a disarm-then-teardown keeps the wake off a destroyed subscriber.
    struct entry
    {
        entry() = default;

        typename Broker::region_handle control;
        typename Broker::region_handle slab;
        broadcast_ring ring;
        std::optional<Notifier> notify;
        std::optional<shm_channel<Notifier>> channel;
        acquire_result verdict = acquire_result::failed;
        int refcount           = 0;
        bool creator           = false;
        // Unset = the wake-drain discards (the send-only default).
        deliver_fn sink;

        // The ONE place the cursor is taken, so the armed wake and the attach-time catch-up
        // share it without racing a second drainer.
        void deliver_pending()
        {
            if(!channel)
                return;
            if(sink)
                channel->drain(sink);
            else
            {
                deliver_fn discard = [](::plexus::wire_bytes<shm_slot_owner>) {};
                channel->drain(discard);
            }
        }
    };

    template<typename R, typename E>
    friend ::plexus::shm::acquire_result detail::topic_open_ring(R &, E &, const std::string &, ::plexus::shm::ring_direction, std::uint32_t, ::plexus::shm::ring_geometry_mode,
                                                                 std::uint32_t, ::plexus::shm::acquire_mode);
    template<typename R, typename E>
    friend ::plexus::shm::acquire_result detail::topic_mint(R &, E &, const std::string &, const ::plexus::shm::ring_geometry &, std::uint64_t);
    template<typename R, typename E>
    friend ::plexus::shm::acquire_result detail::topic_join(R &, E &, const std::string &, const std::string &);
    template<typename R>
    friend bool detail::topic_region_exists(R &, const std::string &);

    // Non-negotiable ordering: disarm the notifier BEFORE the channel (and the subscriber its
    // drain touches) is destroyed, so a wake racing teardown can never post a drain onto a
    // destroyed subscriber.
    void teardown(entry &e) noexcept
    {
        if(e.notify)
            e.notify->disarm();
        e.channel.reset();
    }

    void teardown_all() noexcept
    {
        for(auto &[k, e] : m_entries)
            teardown(*e);
        m_entries.clear();
    }

    Broker &m_broker;
    io::reliability m_reliability;
    io::congestion m_congestion;
    notifier_binder m_bind_notifier;
    std::uint64_t m_max_ring_slab_bytes;
    std::string m_region_ns;
    acquire_failure m_last_failure;

    std::unordered_map<key, std::unique_ptr<entry>, key_hash> m_entries;
};

}

#endif
