#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_TOPIC_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_TOPIC_REGISTRY_H

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/notifier_concept.h"
#include "plexus/io/shm/region_broker_concept.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_slot_owner.h"
#include "plexus/io/shm/shm_acquire_result.h"
#include "plexus/io/shm/detail/shm_topic_open.h"

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

namespace plexus::io::shm {

// over-limit: one cohesive ring-lifecycle owner; the acquire/release/sink/drain verbs and the
// nested entry all share the m_entries ownership table + the per-entry notifier-arm/teardown
// ordering, so splitting the surface scatters that shared lifecycle state (the result types are
// extracted to shm_acquire_result.h, the open/mint/join glue to detail/shm_topic_open.h).
//
// The demand-driven lifecycle owner for the same-host shared-memory rings: the sole owner of
// every live (topic, direction) ring. A first acquire MINTS the ring (or ATTACHES to a peer's),
// each further acquire of the same key bumps a refcount, and the matching releases tear it down
// at 1 -> 0 (the creator also unlinks). It holds NO event-loop type and NO serialization
// primitive of its own — it borrows the region broker by reference and owns one notifier per
// entry (the wakeup MECHANISM satisfies the notifier seam; the event loop, if any, lives in the
// adapter that constructs THIS registry). Borrows the broker BY REFERENCE; non-copy/non-move.
// Templated on the broker + notifier seams so core pulls no POSIX/asio header; the open/mint/
// join glue is extracted to detail/shm_topic_open.h, the result types to shm_acquire_result.h.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_topic_registry
{
public:
    using broker_type = Broker;
    using deliver_fn =
            plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // Constructs each entry's notifier in place over the ring's in-region generation word once
    // the ring is bound. A notifier that wakes on a cross-process futex (the asio reactor
    // bridge) is NOT default-constructible — it binds to the word + the user's executor — so the
    // binder injects that construction without coupling the registry to a concrete notifier ctor.
    using notifier_binder = plexus::detail::move_only_function<void(
            std::optional<Notifier> &, std::atomic<std::uint32_t> &, std::atomic<std::uint32_t> &)>;

    // The default binder: emplace a default-constructed notifier (the stub/recording seam, which
    // wakes nothing so it needs no word); a real reactor bridge injects a binder over the word.
    [[nodiscard]] static notifier_binder default_notifier_binder() noexcept
    {
        return [](std::optional<Notifier> &slot, std::atomic<std::uint32_t> &,
                  std::atomic<std::uint32_t> &) { slot.emplace(); };
    }

    // max_ring_slab_bytes is the node-level per-ring slab ceiling enforced at registration —
    // required-with-default the shipped k_max_ring_slab_bytes so a caller that threads no node
    // knob keeps the shipped bound. The acquire fails closed above it.
    shm_topic_registry(Broker &broker, reliability rel, congestion cong,
                       notifier_binder bind_notifier       = default_notifier_binder(),
                       std::uint64_t   max_ring_slab_bytes = k_max_ring_slab_bytes,
                       std::string     region_ns           = {}) noexcept
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

    ~shm_topic_registry() { teardown_all(); }

    // Demand-drives the ring for (fqn, direction). A repeat acquire of a live key
    // bumps the refcount and returns the same created/attached verdict it minted
    // with. A first acquire mints (or attaches to a peer's) ring of the geometry
    // max_payload sizes (0 -> default).
    acquire_result acquire(const std::string &fqn, ring_direction direction,
                           std::uint32_t      max_payload,
                           ring_geometry_mode mode = ring_geometry_mode::reliable_preserving,
                           std::uint32_t      consumer_capacity = 0,
                           acquire_mode       amode             = acquire_mode::reclaim_stale)
    {
        const key k{fqn, direction};
        if(auto it = m_entries.find(k); it != m_entries.end())
        {
            ++it->second->refcount;
            return it->second->verdict;
        }

        auto e                       = std::make_unique<entry>();
        m_last_failure               = acquire_failure{};
        const acquire_result verdict = detail::topic_open_ring(
                *this, *e, fqn, direction, max_payload, mode, consumer_capacity, amode);
        if(verdict == acquire_result::failed)
            return acquire_result::failed;

        // The ring is now bound: construct the notifier over its generation word, build the
        // channel (its subscriber registers a cursor over the live ring), and arm the notifier
        // with a drain callback the bridge posts onto the user's executor on each wake (the
        // callback borrows the pinned entry by pointer).
        m_bind_notifier(e->notify, e->ring.notify_generation(), e->ring.park_state());
        e->channel.emplace(e->ring, *e->notify, m_reliability, m_congestion);
        e->verdict  = verdict;
        e->refcount = 1;
        entry *raw  = e.get();
        // The ONE drain authority for this ring's single consumer cursor: it DELIVERS to the
        // entry's consumer sink when one has attached, else DISCARDS. Routing both through the
        // one armed drain keeps the single cursor un-raced (a second drainer would steal
        // messages); only the sink varies, never a competing take().
        e->notify->arm([raw] { raw->deliver_pending(); });
        m_entries.emplace(k, std::move(e));
        return verdict;
    }

    // Drops one reference to (fqn, direction). At 1 -> 0 the entry tears down in the mandated
    // order: the notifier is disarmed BEFORE the channel (and the subscriber its drain touches)
    // is destroyed, so a wake in flight can never touch a destroyed subscriber.
    void release(const std::string &fqn, ring_direction direction)
    {
        const key k{fqn, direction};
        auto      it = m_entries.find(k);
        if(it == m_entries.end())
            return;
        if(--it->second->refcount > 0)
            return;
        teardown(*it->second);
        m_entries.erase(it);
    }

    // Install a consumer sink for (fqn, direction): the entry's armed wake-drain hands each
    // pending framed message here (zero-copy) instead of discarding, and any already-queued
    // message is delivered NOW so a consumer attaching after the producer published misses
    // nothing. The sink is destroyed with the entry at teardown — no dangling drain registration.
    void set_consumer_sink(const std::string &fqn, ring_direction direction, deliver_fn sink)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it == m_entries.end())
            return;
        it->second->sink = std::move(sink);
        it->second->deliver_pending();
    }

    // Drop the consumer sink for (fqn, direction): the wake-drain returns to discarding. Called
    // on the receive companion's teardown BEFORE its ring release (disarm-before-destroy).
    void clear_consumer_sink(const std::string &fqn, ring_direction direction)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it != m_entries.end())
            it->second->sink = {};
    }

    // The live channel for a key, or nullptr if none is acquired.
    shm_channel<Notifier> *channel_for(const std::string &fqn, ring_direction direction)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it == m_entries.end() || !it->second->channel)
            return nullptr;
        return &*it->second->channel;
    }

    // Walks every live channel and drains each pending message zero-copy.
    void drain_channels(deliver_fn &deliver)
    {
        for(auto &[k, e] : m_entries)
            if(e->channel)
                e->channel->drain(deliver);
    }

    // Idempotent flush: drain every channel discarding the messages (the teardown
    // sweep). Safe to call repeatedly -- an empty ring drains to nothing.
    void drain()
    {
        deliver_fn discard = [](::plexus::wire_bytes<shm_slot_owner>) {};
        drain_channels(discard);
    }

    std::size_t live_count() const noexcept { return m_entries.size(); }

    // The diagnostic of the most recent fail-closed acquire: which bound was hit plus the exact
    // ask vs available. The mux member surfaces it on a failed dial so a publisher learns WHY
    // the ring could not be provisioned. bound == none after a successful acquire.
    [[nodiscard]] const acquire_failure &last_acquire_failure() const noexcept
    {
        return m_last_failure;
    }

    // Apply the node-level per-ring slab ceiling; it bounds the slab of every ring minted AFTER
    // it is set.
    void set_max_ring_slab_bytes(std::uint64_t bytes) noexcept { m_max_ring_slab_bytes = bytes; }

private:
    struct key
    {
        std::string    fqn;
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
            return std::hash<std::string>{}(k.fqn) ^
                    static_cast<std::size_t>(k.direction) * 0x9e3779b9u;
        }
    };

    // One live ring's owned state, pinned in place by the entry's unique_ptr so the non-movable
    // ring/channel/notifier hold stable addresses. The channel is built LAST (emplaced only
    // AFTER the ring is bound): its slot_subscriber registers a cursor at construction, so the
    // ring must already be created/attached. The channel destructs BEFORE the notifier so a
    // disarm-then-teardown keeps the wake off a destroyed subscriber.
    struct entry
    {
        entry() = default;

        typename Broker::region_handle       control;
        typename Broker::region_handle       slab;
        broadcast_ring                       ring;
        std::optional<Notifier>              notify;
        std::optional<shm_channel<Notifier>> channel;
        acquire_result                       verdict  = acquire_result::failed;
        int                                  refcount = 0;
        bool                                 creator  = false;
        // The consumer delivery sink. Unset = the wake-drain discards (the send-only default).
        // Destroyed with the entry at teardown, so no wake can deliver onto a freed sink.
        deliver_fn sink;

        // Drain every pending message off the single consumer cursor into the sink (or discard
        // when none is set). The ONE place the cursor is taken, so the armed wake and the
        // attach-time catch-up share it without racing a second drainer.
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
    friend ::plexus::io::shm::acquire_result
    detail::topic_open_ring(R &, E &, const std::string &, ::plexus::io::shm::ring_direction,
                            std::uint32_t, ::plexus::io::shm::ring_geometry_mode, std::uint32_t,
                            ::plexus::io::shm::acquire_mode);
    template<typename R, typename E>
    friend ::plexus::io::shm::acquire_result
    detail::topic_mint(R &, E &, const std::string &, const ::plexus::io::shm::ring_geometry &,
                       std::uint64_t);
    template<typename R, typename E>
    friend ::plexus::io::shm::acquire_result detail::topic_join(R &, E &, const std::string &,
                                                                const std::string &);
    template<typename R>
    friend bool detail::topic_region_exists(R &, const std::string &);

    // The teardown ordering (non-negotiable): disarm the notifier BEFORE the channel (and the
    // subscriber its drain touches) is destroyed, so a wake racing teardown can never post a
    // drain onto a destroyed subscriber.
    void teardown(entry &e) noexcept
    {
        if(e.notify)
            e.notify->disarm(); // stop the wake FIRST
        e.channel.reset();      // THEN destroy the channel (its subscriber the drain touches)
        // The ring + region handles destruct when the unique_ptr is erased; with the
        // notifier already disarmed and the subscriber already gone, no drain can fire
        // onto destroyed state.
    }

    void teardown_all() noexcept
    {
        for(auto &[k, e] : m_entries)
            teardown(*e);
        m_entries.clear();
    }

    Broker         &m_broker;
    reliability     m_reliability;
    congestion      m_congestion;
    notifier_binder m_bind_notifier;
    std::uint64_t   m_max_ring_slab_bytes;
    std::string     m_region_ns;
    acquire_failure m_last_failure;

    std::unordered_map<key, std::unique_ptr<entry>, key_hash> m_entries;
};

}

#endif
