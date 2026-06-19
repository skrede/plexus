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

// The outcome of an acquire: a topic ring is either freshly MINTED by this node
// (created -- it owns the unlink), ATTACHED to a peer's existing ring (it never
// unlinks), or FAILED (the broker could not map a region; the caller falls back to
// the wire). Leads with created exactly as the status families lead with ok.
enum class acquire_result : std::uint8_t
{
    created,
    attached,
    failed,
};

// How an acquire resolves a name collision with an existing region.
//
// reclaim_stale is the per-peer dial/listen default: a create that races an existing
// name reclaims it via unlink-then-create, on the assumption the name is a crashed prior
// creator's orphan (the single-owner dial ring has exactly one creator, so a live
// collision is not expected). join_live is the demand-driven COMPANION-convergence policy:
// two LIVE co-host peers independently acquire the SAME deterministically-named ring, so a
// create must NEVER unlink (clobbering the peer's live region would split the pair onto two
// physical rings — one keeps writing the orphaned mapping, the other drains an empty fresh
// one). join_live attaches FIRST when the region exists, and mints the ring itself only
// when none does yet; whichever peer arrives second JOINs, so both converge on the one ring.
enum class acquire_mode : std::uint8_t
{
    reclaim_stale,
    join_live,
};

// Which bound a fail-closed acquire hit. A reliable_preserving ring that cannot be
// provisioned NEVER silently downgrades to best_effort and NEVER silently sizes an
// unbounded region: the registration fails closed and names the bound here so the
// caller (and the operator log) learns WHY, not just that it failed.
//   none         -> no failure recorded (the success default).
//   slab_ceiling -> the ring's required bytes exceed the node-level slab ceiling.
//   os_allocator -> the broker / OS could not map the region (too large for the
//                   broker's region ceiling, denied, or any other mapping failure).
enum class acquire_bound : std::uint8_t
{
    none,
    slab_ceiling,
    os_allocator,
};

// The unified fail-closed diagnostic a failed acquire records: which bound was hit,
// the exact ask (the ring's required slab bytes), and the available limit — the
// node-level slab ceiling for the ceiling bound, or the broker's region_status verdict
// for the OS-allocator bound. ONE surface for "a reliable_preserving ring that could
// not be provisioned," whether the node ceiling or the OS allocator was the wall.
struct acquire_failure
{
    acquire_bound bound       = acquire_bound::none;
    std::uint64_t ask_bytes   = 0;
    std::uint64_t limit_bytes = 0;                 // the slab ceiling (slab_ceiling bound only)
    region_status broker      = region_status::ok; // the OS verdict (os_allocator bound only)
};

// The demand-driven lifecycle owner for the same-host shared-memory rings. It is
// the sole owner of every live (topic, direction) ring: a first acquire MINTS the
// ring (or ATTACHES to a peer's), each further acquire of the same key bumps a
// refcount, and the matching releases tear it down at 1 -> 0 (the creator also
// unlinks). It is the central reshape vs a reactor-serialized model: it holds NO
// event-loop type and NO serialization primitive of its own -- it borrows the
// region broker by reference and owns one notifier per entry (the wakeup MECHANISM
// -- futex primitive or reactor bridge -- satisfies the notifier seam; the event
// loop, if any, lives in the adapter that constructs THIS registry, never here).
// Drain callbacks are the move-only callback wrapper, never the copyable one.
//
//   acquire(fqn, dir, max_payload)  demand-drive the ring for a (topic, direction):
//                                   created on the first mint, attached on a peer
//                                   collision, failed on a broker error. The
//                                   PUBLISHER sizes the ring via max_payload; a
//                                   subscriber-only acquire (max_payload == 0) falls
//                                   back to the default ring geometry.
//   release(fqn, dir)               drop one reference; at 1 -> 0 the entry tears
//                                   down (notifier disarmed BEFORE the channel +
//                                   its subscriber are destroyed -- the
//                                   non-negotiable teardown ordering) and a creator
//                                   unlinks the region.
//   channel_for(fqn, dir)           the live channel for a key (nullptr if none) --
//                                   the send/drain surface for that ring.
//   drain_channels(deliver)         walk every live channel and hand each pending
//                                   message up zero-copy via the deliver callback.
//   drain()                         idempotent alias of drain_channels with a
//                                   no-op deliver (used to flush on teardown).
//
// Borrows the broker BY REFERENCE; non-copy/non-move owning service (the sole
// lifecycle owner). Templated on the broker + notifier seams so core pulls no
// POSIX/asio header.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_topic_registry
{
public:
    using deliver_fn =
            plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // The notifier-binder: constructs each entry's notifier in place over the ring's
    // in-region generation word once the ring is bound. A notifier that wakes on a
    // cross-process futex (the asio reactor bridge) is NOT default-constructible — it
    // binds to the word + the user's executor — so the registry cannot default-build it
    // in the entry. The binder injects that construction without coupling the registry
    // to a concrete notifier's ctor: the default emplaces a default-constructed notifier
    // (the recording/stub seam), and the asio composition injects a binder that captures
    // the io_context and emplaces the reactor bridge over (executor, word).
    using notifier_binder = plexus::detail::move_only_function<void(
            std::optional<Notifier> &, std::atomic<std::uint32_t> &, std::atomic<std::uint32_t> &)>;

    // The default binder: emplace a default-constructed notifier, ignoring the word.
    // The stub/recording notifier the unit oracles use is default-constructible and
    // wakes nothing, so it needs no word; a real reactor bridge injects a binder that
    // captures the executor and emplaces over (executor, word).
    [[nodiscard]] static notifier_binder default_notifier_binder() noexcept
    {
        return [](std::optional<Notifier> &slot, std::atomic<std::uint32_t> &,
                  std::atomic<std::uint32_t> &) { slot.emplace(); };
    }

    // max_ring_slab_bytes is the node-level per-ring slab ceiling enforced at
    // registration — required-with-default the shipped k_max_ring_slab_bytes so a
    // caller that threads no node knob keeps the shipped bound. plan 03 fails closed
    // above it; this plan threads it so the enforced ceiling is the node value.
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

        auto e         = std::make_unique<entry>();
        m_last_failure = acquire_failure{};
        const acquire_result verdict =
                open_ring(*e, fqn, direction, max_payload, mode, consumer_capacity, amode);
        if(verdict == acquire_result::failed)
            return acquire_result::failed;

        // The ring is now bound: construct the notifier over the ring's generation word
        // (the binder injects an executor for a reactor bridge, or default-builds a stub),
        // build the channel (its subscriber registers a cursor over the live ring), and arm
        // the notifier with a drain-this-channel callback. On each cross-process wake the
        // bridge posts this drain onto the user's executor; the callback borrows the pinned
        // entry by pointer.
        m_bind_notifier(e->notify, e->ring.notify_generation(), e->ring.park_state());
        e->channel.emplace(e->ring, *e->notify, m_reliability, m_congestion);
        e->verdict  = verdict;
        e->refcount = 1;
        entry *raw  = e.get();
        // The ONE drain authority for this ring's single consumer cursor: on each
        // cross-process wake the bridge posts this onto the user's executor. It DELIVERS
        // to the entry's consumer sink when a consumer has attached (the same-host receive
        // companion), else it DISCARDS — byte-identical to the send-only path. Routing both
        // through the one armed drain keeps the single cursor un-raced (a second cursor
        // draining the same entry would steal messages); the sink is the only thing that
        // varies, never a competing take().
        e->notify->arm([raw] { raw->deliver_pending(); });
        m_entries.emplace(k, std::move(e));
        return verdict;
    }

    // Drops one reference to (fqn, direction). At 1 -> 0 the entry tears down in the
    // mandated order: the notifier is disarmed BEFORE the channel (and the
    // subscriber its drain touches) is destroyed, so a wake in flight can never
    // touch a destroyed subscriber.
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

    // Install a consumer sink for (fqn, direction): the entry's armed wake-drain hands
    // each pending framed message here (zero-copy) instead of discarding, and any message
    // already queued is delivered NOW so a consumer attaching after the producer published
    // misses nothing. Used by the same-host receive companion to drain the co-host ring into
    // the node receive path. A no-op for an unheld key. The sink is destroyed with the entry
    // at 1->0/teardown, so a release drops it — no dangling drain registration.
    void set_consumer_sink(const std::string &fqn, ring_direction direction, deliver_fn sink)
    {
        auto it = m_entries.find(key{fqn, direction});
        if(it == m_entries.end())
            return;
        it->second->sink = std::move(sink);
        it->second->deliver_pending();
    }

    // Drop the consumer sink for (fqn, direction): the entry's wake-drain returns to
    // discarding. A no-op for an unheld key or one with no sink. Called on the receive
    // companion's teardown BEFORE its ring release, mirroring the disarm-before-destroy order.
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

    // The diagnostic of the most recent fail-closed acquire: which bound was hit
    // (slab ceiling vs OS allocator) plus the exact ask vs available. The mux member
    // surfaces it on a failed dial so a publisher learns WHY the ring could not be
    // provisioned, never a silent downgrade. bound == none after a successful acquire.
    [[nodiscard]] const acquire_failure &last_acquire_failure() const noexcept
    {
        return m_last_failure;
    }

    // Apply the node-level per-ring slab ceiling. Called once by the node owner at
    // construction so the enforced ceiling is the node value rather than the shipped
    // compile-time default; it bounds the slab of every ring minted AFTER it is set.
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

    // One live ring's owned state, pinned in place by the entry's unique_ptr so the
    // non-movable ring/channel/notifier hold stable addresses. The channel is built
    // LAST (optional, emplaced only AFTER open_ring binds the ring over the mapped
    // regions): its slot_subscriber registers a cursor at construction, so the ring
    // must already be created/attached -- it borrows the sibling ring + notifier by
    // reference, both stable under the unique_ptr. The channel destructs BEFORE the
    // notifier (the optional resets first in the dtor sweep), so a disarm-then-
    // teardown keeps the wake off a destroyed subscriber.
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
        // The consumer delivery sink. Unset = the wake-drain discards (the send-only
        // default). Set by set_consumer_sink when a same-host receive companion attaches;
        // destroyed with the entry at teardown, so no wake can deliver onto a freed sink.
        deliver_fn sink;

        // Drain every pending message off the single consumer cursor into the sink (or
        // discard when no sink is set). The ONE place the cursor is taken, so the armed
        // wake and the attach-time catch-up share it without racing a second drainer.
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

    // Mints or attaches the two regions for (fqn, direction) and binds the entry's
    // ring over them. A create that finds a live name re-issues as attach (the
    // demand-driven collision probe). The geometry comes from max_payload (0 -> default).
    //
    // amode decides the collision policy. reclaim_stale unlinks an existing name on create
    // (the single-owner dial ring reclaims a crashed creator's orphan). join_live NEVER
    // unlinks: a live co-host peer already mapped this companion ring, so clobbering it would
    // split the pair onto two physical rings (one keeps writing the orphaned mapping, the
    // other drains an empty fresh one). join_live attaches FIRST when the region exists, and
    // mints the ring itself only when none does yet; the peer arriving second JOINs, so both
    // converge on the one ring (demand-driven convergence with no clobber).
    acquire_result open_ring(entry &e, const std::string &fqn, ring_direction direction,
                             std::uint32_t max_payload, ring_geometry_mode mode,
                             std::uint32_t consumer_capacity, acquire_mode amode)
    {
        const std::string                  ctrl_name = region_name_for(fqn, direction, m_region_ns);
        const std::string                  slab_name = ctrl_name + ".s";
        const std::optional<std::uint32_t> want =
                max_payload == 0 ? std::nullopt : std::optional<std::uint32_t>{max_payload};
        const std::uint64_t capacity = consumer_capacity == 0 ? k_max_consumers : consumer_capacity;
        const ring_geometry geom     = ring_geometry_for(want, mode, consumer_capacity);

        // The ceiling leg of the unified fail-closed gate is a PURE query (no broker
        // touch): reject an over-ceiling reliable ring before any region is minted, so
        // an oversize declaration never even orphans the control region. The ask is the
        // slab the ring would size (the slab dominates; the control region is bounded).
        const std::uint64_t ask = slab_region_bytes(geom.cell_count, geom.slot_capacity);
        if(ask > m_max_ring_slab_bytes)
        {
            m_last_failure = acquire_failure{acquire_bound::slab_ceiling, ask,
                                             m_max_ring_slab_bytes, region_status::ok};
            return acquire_result::failed;
        }

        // join_live attaches FIRST: when a co-host peer already mapped the ring, JOIN it
        // outright (never even attempt a create that could clobber). Only when no region
        // exists yet does it fall through to a non-unlinking create.
        if(amode == acquire_mode::join_live && region_exists(ctrl_name))
            return join(e, ctrl_name, slab_name);

        create_options opts;
        opts.unlink_stale_on_create = amode == acquire_mode::reclaim_stale;
        const region_status cs =
                m_broker.create(ctrl_name, control_region_bytes(geom.cell_count), opts, e.control);
        if(cs == region_status::ok)
            return mint(e, slab_name, geom, capacity);
        if(cs == region_status::already_exists)
            return join(e, ctrl_name, slab_name);
        m_last_failure = acquire_failure{acquire_bound::os_allocator,
                                         control_region_bytes(geom.cell_count), 0, cs};
        return acquire_result::failed;
    }

    // A cheap existence probe for the consumer attach-first path: a successful read-only
    // attach proves the region is live (the handle is discarded — the real join re-attaches
    // both regions and binds the ring). A miss returns false (the consumer then mints).
    [[nodiscard]] bool region_exists(const std::string &ctrl_name)
    {
        typename Broker::region_handle probe;
        return m_broker.attach(ctrl_name, probe) == region_status::ok;
    }

    // The creator path: it owns both regions, stamps a fresh ring at the resolved
    // consumer capacity, and unlinks on teardown.
    acquire_result mint(entry &e, const std::string &slab_name, const ring_geometry &geom,
                        std::uint64_t consumer_capacity)
    {
        // The OS-allocator leg of the unified fail-closed gate: the ceiling was already
        // cleared in open_ring (a pure query), so a slab create that the broker / OS
        // cannot map fails closed naming the OS-ALLOCATOR bound with the exact ask and
        // the broker's verdict. NEVER an auto-downgrade and NEVER a silently-shrunk ring.
        const std::uint64_t ask = slab_region_bytes(geom.cell_count, geom.slot_capacity);
        if(const region_status cs = m_broker.create(
                   slab_name, ask, create_options{.unlink_stale_on_create = true}, e.slab);
           cs != region_status::ok)
        {
            m_last_failure = acquire_failure{acquire_bound::os_allocator, ask, 0, cs};
            return acquire_result::failed;
        }
        if(broadcast_ring::create(e.control.bytes(), e.slab.bytes(), geom.cell_count,
                                  geom.slot_capacity, e.ring, consumer_capacity) != loan_status::ok)
            return acquire_result::failed;
        e.creator = true;
        return acquire_result::created;
    }

    // The attacher path: it maps the peer's existing regions and re-reads the
    // geometry from the control header (never unlinks).
    acquire_result join(entry &e, const std::string &ctrl_name, const std::string &slab_name)
    {
        if(m_broker.attach(ctrl_name, e.control) != region_status::ok ||
           m_broker.attach(slab_name, e.slab) != region_status::ok)
            return acquire_result::failed;
        if(broadcast_ring::attach(e.control.bytes(), e.slab.bytes(), e.ring) != loan_status::ok)
            return acquire_result::failed;
        return acquire_result::attached;
    }

    // The teardown ordering (non-negotiable): disarm the notifier BEFORE the channel
    // (and the subscriber its drain touches) is destroyed, so a wake racing the
    // teardown can never post a drain onto a destroyed subscriber.
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
