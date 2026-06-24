#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBER_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBER_REGISTRY_H

#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/keyed_refcount.h"
#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/detail/subscriber_dispatch.h"
#include "plexus/topic_qos.h"

#include <string>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The passive per-fqn subscriber state the forwarder owns directly: no topic-listener
// indirection, no framed-byte cache. It holds, per fqn (keyed by the topic_hash), the subscribed
// peers (registry_subscriber) and the per-(peer, fqn) refcount that gates attach/detach. Every
// structure is grown at attach (setup); the fan-out loop only reads, never inserts. Channel is the
// Policy's byte_channel_type.
template<typename Channel>
class subscriber_registry
{
public:
    using subscriber         = detail::registry_subscriber<Channel>;
    using band_drop_counters = detail::band_drop_counters;
    using topic_entry        = detail::registry_topic_entry<Channel>;

    // Bump the (peer, fqn) refcount; returns the post-increment count. The 0->1
    // result is the gate the forwarder emits a subscribe on.
    std::uint32_t bump_refcount(std::string_view node_name, std::string_view fqn)
    {
        return m_refcount.bump(node_name, fqn);
    }

    // Drop the (peer, fqn) refcount; returns the post-decrement count. The 1->0
    // result is the gate the forwarder emits an unsubscribe on. Returns a large
    // sentinel when the pair is unknown so the caller treats it as "no transition".
    std::uint32_t drop_refcount(std::string_view node_name, std::string_view fqn)
    {
        return m_refcount.drop(node_name, fqn);
    }

    void add_subscriber(std::uint64_t topic_hash, std::string_view fqn, Channel &channel, std::string_view node_name, const subscriber_qos &qos = subscriber_qos{},
                        std::optional<std::uint64_t> type_id = std::nullopt)
    {
        detail::add_topic_subscriber(m_topics, topic_hash, fqn, channel, node_name, qos, type_id);
    }

    // The subscriber's stored QoS for a (topic_hash, channel) pair, or the friendly default when
    // the pair is unknown — absence is the default choice (not a refusal).
    subscriber_qos qos_for_subscriber(std::uint64_t topic_hash, const Channel &channel) const
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end())
            return subscriber_qos{};
        for(const auto &sub : it->second.subscribers)
            if(&sub.channel.get() == &channel)
                return sub.sub_qos;
        return subscriber_qos{};
    }

    // Record a publisher-declared per-topic qos (and the topic_hash -> fqn
    // resolution) BEFORE any subscriber attaches. A later add_subscriber for the
    // same hash leaves this qos intact (it only fills an empty fqn). An optional
    // producer type_id (std::nullopt = undeclared) is recorded alongside the qos —
    // the subscribe-time match authority.
    void declare(std::uint64_t topic_hash, std::string_view fqn, topic_qos qos, std::optional<std::uint64_t> producer_type_id = std::nullopt, bool emit_source_identity = false)
    {
        auto &entry = m_topics[topic_hash];
        if(entry.fqn.empty())
            entry.fqn = std::string{fqn};
        entry.qos                  = qos;
        entry.producer_type_id     = producer_type_id;
        entry.emit_source_identity = emit_source_identity;
        // Mint the endpoint counter ONCE; a re-declare keeps it so the endpoint's gid is stable.
        // The monotonic allocator is a registry member — no static singleton, no hot-path RNG.
        if(emit_source_identity && !entry.endpoint_counter)
            entry.endpoint_counter = m_next_endpoint_counter++;
    }

    // The per-topic qos, or a default topic_qos{} when the hash is unknown.
    topic_qos qos_for(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? topic_qos{} : it->second.qos;
    }

    // The producer-declared type_id for a topic, or std::nullopt when the topic declared no type
    // (a distinct state from a zero type_id — never refused against an undeclared producer type).
    std::optional<std::uint64_t> producer_type_id(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? std::nullopt : it->second.producer_type_id;
    }

    // The producer-declared source-identity OFFER (the capability advertisement the
    // request-vs-offered relation reads), true iff the topic declared emit_source_identity.
    bool offers_source_identity(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it != m_topics.end() && it->second.emit_source_identity;
    }

    // Drop one peer's fan-out entry for an fqn.
    void remove_subscriber(std::uint64_t topic_hash, const Channel &channel)
    {
        detail::remove_topic_subscriber(m_topics, topic_hash, channel);
    }

    // Drop every fan-out entry and refcount for one peer (peer-death path).
    void remove_peer(std::string_view node_name, const Channel &channel)
    {
        detail::remove_peer_subscribers(m_topics, channel);
        m_refcount.forget(node_name);
    }

    // The whole per-topic record in ONE lookup, for the hot publish verbs: reading
    // subscribers/qos/source-identity through the per-field accessors above cost
    // three to four hash finds per publish on the same key. nullptr when the hash
    // names no declared or attached topic.
    const topic_entry *entry_for(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? nullptr : &it->second;
    }

    // The per-topic stamp-demand latch as a read-only query (the hot publish reads the
    // topic_entry field directly). True for an unknown topic — the safe always-on default.
    bool any_subscriber_wants_info(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? true : it->second.any_subscriber_wants_info;
    }

    // Bump the per-(topic, band) drop counter for a cause; a `none` cause is a no-op. The lookup
    // is find-only: minting an empty-fqn entry here would let fqn_for memoize an empty string as a
    // "resolved" view, conflating unknown with known-unnamed.
    void record_drop(std::uint64_t topic_hash, std::size_t band, detail::drop_cause cause)
    {
        if(cause == detail::drop_cause::none || band >= detail::k_egress_bands)
            return;
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end())
            return;
        detail::bump_band_drop(it->second.drops[band], cause);
    }

    // The per-(topic, band, cause) drop tally, read on demand (occupancy style). Returns 0
    // for an unknown topic or out-of-range band.
    std::size_t dropped(std::uint64_t topic_hash, std::size_t band, detail::drop_cause cause) const
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end() || band >= detail::k_egress_bands)
            return 0;
        return detail::read_band_drop(it->second.drops[band], cause);
    }

    // Resolve a wire topic_hash back to its fqn (the receive tail); empty when the hash names no
    // attached topic. The last resolution is memoized (the per-delivery hash find was measurable on
    // the in-process loop). The cached pointer is safe ONLY because m_topics entries are NEVER
    // erased (peer/sub removal empties the subscriber list, never the record) and the node-based
    // map keeps value addresses stable — an erase added here would have to reset the memo.
    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        if(m_last_fqn != nullptr && topic_hash == m_last_fqn_hash)
            return *m_last_fqn;
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end())
            return {};
        m_last_fqn_hash = topic_hash;
        m_last_fqn      = &it->second.fqn;
        return *m_last_fqn;
    }

private:
    std::unordered_map<std::uint64_t, topic_entry> m_topics;
    mutable std::uint64_t                          m_last_fqn_hash{0};
    mutable const std::string                     *m_last_fqn{nullptr};
    detail::keyed_refcount                         m_refcount;
    // The per-node monotonic source-identity endpoint-counter allocator. Minted at
    // declare (cold path), never on the hot path. Starts at 1 so 0 stays free as an
    // "unminted" value if ever needed; gid uniqueness comes from node_id ‖ counter.
    std::uint64_t m_next_endpoint_counter{1};
};

}

#endif
