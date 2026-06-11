#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBER_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBER_REGISTRY_H

#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/topic_qos.h"

#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The passive per-fqn subscriber state the forwarder owns directly: no topic-
// listener indirection, no framed-byte cache. It holds, per fqn (keyed by the
// topic_hash and carrying the fqn string), the set of subscribed peers — each a
// byte_channel reference plus the node-name key it was attached under — and the
// per-(peer, fqn) refcount that gates attach/detach. Every structure is grown at
// attach (setup); the fan-out loop only reads, never inserts.
//
// Channel is the Policy's byte_channel_type. A subscriber is stored as a bare
// channel pointer plus a node-name key for fast identity comparison; the public
// forwarder API stays const&-only — the pointer is an internal storage detail,
// never surfaced.
template <typename Channel>
class subscriber_registry
{
public:
    struct subscriber
    {
        Channel *channel;
        std::string node_name;
        locality tier;   // the delivery tier, classified ONCE at attach (never per fan-out hop)
        // The subscriber's QoS choice, stored ONCE at attach alongside the tier and
        // read at replay — never mutated on the hot path. An absent wire region
        // lands here as the friendly default (a real choice, not a sentinel).
        subscriber_qos qos{};
        // The subscriber-declared type identity, stored ONCE at attach. std::nullopt is
        // a distinct "undeclared type" state (NOT a zero type_id) so an untyped
        // subscriber is never matched as if it had claimed type 0 — it gates the typed
        // fast-path eligibility, mirroring producer_type_id's absence discipline.
        std::optional<std::uint64_t> type_id;
    };

    struct topic_entry
    {
        std::string fqn;
        std::vector<subscriber> subscribers;
        topic_qos qos{};
        // The producer-declared type identity for this topic. std::nullopt is a
        // distinct "undeclared type" state (NOT a zero type_id) so an unknown
        // producer type never false-refuses a subscriber — matching applies only
        // when BOTH sides declare a type. The type_name (a future graph-layer
        // concern) is intentionally not recorded here; matching authority is the
        // type_id alone.
        std::optional<std::uint64_t> producer_type_id;
        // Producer-declared source-identity carriage (Option 2: producer-offered).
        // When emit_source_identity is set, this topic's publishes carry the gid flag
        // + a varint endpoint_counter; the receiver reconstructs publisher_gid as
        // session.node_id ‖ endpoint_counter. The counter is minted ONCE at the first
        // such declare (std::nullopt until then) and is STABLE thereafter — it does
        // NOT change on re-declare or across reconnect (the registry record persists),
        // so an endpoint's gid is stable per IDENT-02.
        bool emit_source_identity{false};
        std::optional<std::uint64_t> endpoint_counter;
    };

    // Bump the (peer, fqn) refcount; returns the post-increment count. The 0->1
    // result is the gate the forwarder emits a subscribe on.
    std::uint32_t bump_refcount(std::string_view node_name, std::string_view fqn)
    {
        auto &per_peer = m_refcount[std::string{node_name}];
        auto [it, inserted] = per_peer.try_emplace(std::string{fqn}, 0u);
        return ++it->second;
    }

    // Drop the (peer, fqn) refcount; returns the post-decrement count. The 1->0
    // result is the gate the forwarder emits an unsubscribe on. Returns a large
    // sentinel when the pair is unknown so the caller treats it as "no transition".
    std::uint32_t drop_refcount(std::string_view node_name, std::string_view fqn)
    {
        auto peer_it = m_refcount.find(std::string{node_name});
        if(peer_it == m_refcount.end())
            return k_no_entry;
        auto fqn_it = peer_it->second.find(std::string{fqn});
        if(fqn_it == peer_it->second.end())
            return k_no_entry;
        std::uint32_t remaining = --fqn_it->second;
        if(remaining == 0)
        {
            peer_it->second.erase(fqn_it);
            if(peer_it->second.empty())
                m_refcount.erase(peer_it);
        }
        return remaining;
    }

    // Register a peer's fan-out entry for an fqn (idempotent on a re-add of the
    // same channel). Records the topic_hash -> fqn resolution the receive tail
    // reads. Called at attach only. The qos has two distinct sources sharing this
    // store: the SUBSCRIBE side passes the subscriber's OWN requested choice (the
    // local monitor reads it back via qos_for_subscriber to arm the deadline/lease),
    // and the producer-side attach_for_fanout passes the subscriber's request it
    // learned off the wire for fan-out — the same slot, two callers.
    void add_subscriber(std::uint64_t topic_hash, std::string_view fqn,
                        Channel &channel, std::string_view node_name,
                        const subscriber_qos &qos = subscriber_qos{},
                        std::optional<std::uint64_t> type_id = std::nullopt)
    {
        m_hash_to_fqn[topic_hash] = std::string{fqn};
        auto &entry = m_topics[topic_hash];
        if(entry.fqn.empty())
            entry.fqn = std::string{fqn};
        for(const auto &sub : entry.subscribers)
            if(sub.channel == &channel)
                return;   // idempotent re-add keeps the first qos
        // Classify the delivery tier ONCE here from the channel's OWN endpoint scheme
        // (the transport that minted it, never peer-supplied data). remote_endpoint() is
        // a syscall on a real socket channel, so it is read at attach and cached — the
        // fan-out loop only reads the stored tier. The qos is stored the same way: once.
        const locality tier = tier_of(channel.remote_endpoint().scheme);
        entry.subscribers.push_back(subscriber{&channel, std::string{node_name}, tier, qos, type_id});
    }

    // The subscriber's stored QoS for a (topic_hash, channel) pair, or the friendly
    // subscriber_qos default when the pair is unknown — absence is the default choice
    // (a subscriber that carried no region genuinely chose the default), not a refusal.
    subscriber_qos qos_for_subscriber(std::uint64_t topic_hash, const Channel &channel) const
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end())
            return subscriber_qos{};
        for(const auto &sub : it->second.subscribers)
            if(sub.channel == &channel)
                return sub.qos;
        return subscriber_qos{};
    }

    // Record a publisher-declared per-topic qos (and the topic_hash -> fqn
    // resolution) BEFORE any subscriber attaches. A later add_subscriber for the
    // same hash leaves this qos intact (it only fills an empty fqn). An optional
    // producer type_id (std::nullopt = undeclared) is recorded alongside the qos —
    // the subscribe-time match authority.
    void declare(std::uint64_t topic_hash, std::string_view fqn, topic_qos qos,
                 std::optional<std::uint64_t> producer_type_id = std::nullopt,
                 bool emit_source_identity = false)
    {
        m_hash_to_fqn[topic_hash] = std::string{fqn};
        auto &entry = m_topics[topic_hash];
        if(entry.fqn.empty())
            entry.fqn = std::string{fqn};
        entry.qos = qos;
        entry.producer_type_id = producer_type_id;
        entry.emit_source_identity = emit_source_identity;
        // Mint the endpoint counter ONCE, the first time this topic declares source
        // identity. A re-declare keeps the existing counter so the endpoint's gid is
        // stable (IDENT-02); the monotonic allocator lives here (a node-level member of
        // the forwarder-owned registry — no static singleton, no hot-path RNG).
        if(emit_source_identity && !entry.endpoint_counter)
            entry.endpoint_counter = m_next_endpoint_counter++;
    }

    // The per-topic qos, or a default topic_qos{} when the hash is unknown.
    topic_qos qos_for(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? topic_qos{} : it->second.qos;
    }

    // The producer-declared type_id for a topic, or std::nullopt when the topic
    // declared no type (undeclared producer type). Absence is a distinct state from
    // a zero type_id — a subscriber type_id is never refused against an undeclared
    // producer type.
    std::optional<std::uint64_t> producer_type_id(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it == m_topics.end() ? std::nullopt : it->second.producer_type_id;
    }

    // The producer-declared source-identity OFFER as a first-class bool — the
    // request-vs-offered source_identity relation reads it to decide whether a
    // subscriber that requires source identity may attach. Distinct from
    // source_identity_counter (which is the per-frame endpoint counter): this is the
    // capability advertisement, true iff the topic declared emit_source_identity.
    bool offers_source_identity(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        return it != m_topics.end() && it->second.emit_source_identity;
    }

    // The source-identity endpoint counter to emit for a topic, or std::nullopt when
    // the topic did not declare source identity (so publish emits 0 B and a byte-
    // identical v3-no-flag frame). Engaged iff the producer declared emit_source_identity.
    std::optional<std::uint64_t> source_identity_counter(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end() || !it->second.emit_source_identity)
            return std::nullopt;
        return it->second.endpoint_counter;
    }

    // Drop one peer's fan-out entry for an fqn.
    void remove_subscriber(std::uint64_t topic_hash, const Channel &channel)
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end())
            return;
        std::erase_if(it->second.subscribers,
                      [&](const subscriber &s) { return s.channel == &channel; });
    }

    // Drop every fan-out entry and refcount for one peer (peer-death path).
    void remove_peer(std::string_view node_name, const Channel &channel)
    {
        for(auto &[hash, entry] : m_topics)
            std::erase_if(entry.subscribers,
                          [&](const subscriber &s) { return s.channel == &channel; });
        m_refcount.erase(std::string{node_name});
    }

    // The fan-out target list for a topic_hash, or nullptr when none subscribe.
    const std::vector<subscriber> *subscribers_for(std::uint64_t topic_hash) const
    {
        auto it = m_topics.find(topic_hash);
        if(it == m_topics.end() || it->second.subscribers.empty())
            return nullptr;
        return &it->second.subscribers;
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

    // Resolve a wire topic_hash back to its fqn (the receive tail). Empty when
    // the hash names no attached topic.
    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        auto it = m_hash_to_fqn.find(topic_hash);
        return it == m_hash_to_fqn.end() ? std::string_view{} : std::string_view{it->second};
    }

private:
    static constexpr std::uint32_t k_no_entry = ~0u;

    std::unordered_map<std::uint64_t, topic_entry> m_topics;
    std::unordered_map<std::uint64_t, std::string> m_hash_to_fqn;
    std::unordered_map<std::string, std::unordered_map<std::string, std::uint32_t>> m_refcount;
    // The per-node monotonic source-identity endpoint-counter allocator. Minted at
    // declare (cold path), never on the hot path. Starts at 1 so 0 stays free as an
    // "unminted" value if ever needed; gid uniqueness comes from node_id ‖ counter.
    std::uint64_t m_next_endpoint_counter{1};
};

}

#endif
