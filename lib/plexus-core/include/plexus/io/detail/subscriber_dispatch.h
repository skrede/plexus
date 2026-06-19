#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_SUBSCRIBER_DISPATCH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_SUBSCRIBER_DISPATCH_H

#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/topic_qos.h"

#include <array>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace plexus::io::detail {

// One peer's fan-out entry for a topic: a bare channel pointer plus a node-name key for fast
// identity comparison (the public forwarder API stays const&-only — the pointer is internal
// storage, never surfaced). The tier and qos are classified ONCE at attach and only read on the
// hot fan-out loop; an absent qos wire region lands as the friendly default (a real choice). An
// absent type_id is a distinct "undeclared type" state (NOT a zero type_id), gating typed
// fast-path eligibility.
template<typename Channel>
struct registry_subscriber
{
    Channel                     *channel;
    std::string                  node_name;
    locality                     tier;
    subscriber_qos               qos{};
    std::optional<std::uint64_t> type_id;
};

// One band's per-cause drop tallies on the per-topic record: fixed cardinality, bumped at the
// fan-out drop site, read on demand (no atomics on the single-threaded egress path, no map, no
// per-drop allocation).
struct band_drop_counters
{
    std::size_t dropped_oldest{0};
    std::size_t dropped_newest{0};
    std::size_t blocked{0};
};

// The passive per-fqn record the forwarder owns directly: the subscribed peers, the per-topic qos
// and producer type identity, the per-band drop tallies, and the source-identity carriage. An
// absent producer_type_id is a distinct "undeclared type" state so an unknown producer type never
// false-refuses a subscriber. emit_source_identity + endpoint_counter implement the producer-
// offered source identity: the counter is minted ONCE at the first such declare and is STABLE
// thereafter (it does NOT change on re-declare or across reconnect), so an endpoint's gid is stable. any_subscriber_wants_info is the OR-reduce stamp-demand latch the hot publish reads
// to gate the source-stamp clock read; it is recomputed at attach/retire (cold), never per
// fan-out, and defaults true on an empty topic (the safe always-on stamping).
template<typename Channel>
struct registry_topic_entry
{
    std::string                                            fqn;
    std::vector<registry_subscriber<Channel>>              subscribers;
    topic_qos                                              qos{};
    std::array<band_drop_counters, detail::k_egress_bands> drops{};
    std::optional<std::uint64_t>                           producer_type_id;
    bool                                                   emit_source_identity{false};
    std::optional<std::uint64_t>                           endpoint_counter;
    bool                                                   any_subscriber_wants_info{true};
};

// OR-reduce the per-topic stamp-demand latch over the entry's subscribers. An empty subscriber set
// latches true (the safe always-on default). Cold path: attach/retire only, never on fan-out.
template<typename Channel>
void recompute_wants_info(registry_topic_entry<Channel> &entry)
{
    if(entry.subscribers.empty())
    {
        entry.any_subscriber_wants_info = true;
        return;
    }
    entry.any_subscriber_wants_info = std::any_of(
            entry.subscribers.begin(), entry.subscribers.end(),
            [](const registry_subscriber<Channel> &s) { return s.qos.wants_message_info; });
}

// Bump one (topic, band, cause) drop tally; a `none` cause or an out-of-range band is a no-op.
inline void bump_band_drop(band_drop_counters &c, detail::drop_cause cause) noexcept
{
    switch(cause)
    {
        case detail::drop_cause::drop_oldest: ++c.dropped_oldest; return;
        case detail::drop_cause::drop_newest: ++c.dropped_newest; return;
        case detail::drop_cause::blocked:     ++c.blocked; return;
        // The receive-side datagram causes carry their own occupancy counters at the datagram
        // sites — this per-(topic,band) egress table holds the overflow trio only.
        default: return;
    }
}

// Read one (topic, band, cause) drop tally; an unrecognized cause reads 0.
inline std::size_t read_band_drop(const band_drop_counters &c, detail::drop_cause cause) noexcept
{
    switch(cause)
    {
        case detail::drop_cause::drop_oldest: return c.dropped_oldest;
        case detail::drop_cause::drop_newest: return c.dropped_newest;
        case detail::drop_cause::blocked:     return c.blocked;
        default:                              return 0;
    }
}

// Register a peer's fan-out entry for an fqn (idempotent on a re-add of the same channel), and
// record the topic_hash -> fqn resolution the receive tail reads. Called at attach only. The tier
// is classified ONCE here from the channel's OWN endpoint scheme (never peer-supplied data;
// remote_endpoint() is a syscall on a real socket channel, read once and cached). The qos has two
// callers sharing this store: the SUBSCRIBE side passes the subscriber's own requested choice, and
// the producer-side attach_for_fanout passes the request it learned off the wire — the same slot.
template<typename Map, typename Channel>
void add_topic_subscriber(Map &topics, std::uint64_t topic_hash, std::string_view fqn,
                          Channel &channel, std::string_view node_name, const subscriber_qos &qos,
                          std::optional<std::uint64_t> type_id)
{
    auto &entry = topics[topic_hash];
    if(entry.fqn.empty())
        entry.fqn = std::string{fqn};
    for(const auto &sub : entry.subscribers)
        if(sub.channel == &channel)
            return; // idempotent re-add keeps the first qos
    const locality tier = tier_of(channel.remote_endpoint().scheme);
    entry.subscribers.push_back(
            registry_subscriber<Channel>{&channel, std::string{node_name}, tier, qos, type_id});
    recompute_wants_info(entry);
}

// Drop one peer's fan-out entry for an fqn.
template<typename Map, typename Channel>
void remove_topic_subscriber(Map &topics, std::uint64_t topic_hash, const Channel &channel)
{
    auto it = topics.find(topic_hash);
    if(it == topics.end())
        return;
    std::erase_if(it->second.subscribers,
                  [&](const registry_subscriber<Channel> &s) { return s.channel == &channel; });
    recompute_wants_info(it->second);
}

// Drop every fan-out entry for one peer across all topics (peer-death path).
template<typename Map, typename Channel>
void remove_peer_subscribers(Map &topics, const Channel &channel)
{
    for(auto &[hash, entry] : topics)
    {
        std::erase_if(entry.subscribers,
                      [&](const registry_subscriber<Channel> &s) { return s.channel == &channel; });
        recompute_wants_info(entry);
    }
}

}

#endif
