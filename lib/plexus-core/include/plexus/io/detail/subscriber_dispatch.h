#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_SUBSCRIBER_DISPATCH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_SUBSCRIBER_DISPATCH_H

#include "plexus/topic_qos.h"

#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/priority_band_queue.h"

#include <array>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <functional>

namespace plexus::io::detail {

template<typename Channel>
struct registry_subscriber
{
    locality tier;
    std::string node_name;
    subscriber_qos sub_qos{};
    std::optional<std::uint64_t> type_id;
    std::reference_wrapper<Channel> channel;
};

struct band_drop_counters
{
    std::size_t blocked{0};
    std::size_t dropped_oldest{0};
    std::size_t dropped_newest{0};
};

template<typename Channel>
struct registry_topic_entry
{
    topic_qos qos{};
    std::string fqn;
    bool emit_source_identity{false};
    bool any_subscriber_wants_info{true};
    std::optional<std::uint64_t> producer_type_id;
    std::optional<std::uint64_t> endpoint_counter;
    std::vector<registry_subscriber<Channel>> subscribers;
    std::array<band_drop_counters, detail::k_egress_bands> drops{};
};

template<typename Channel>
void recompute_wants_info(registry_topic_entry<Channel> &entry)
{
    if(entry.subscribers.empty())
    {
        entry.any_subscriber_wants_info = true;
        return;
    }
    entry.any_subscriber_wants_info =
            std::any_of(entry.subscribers.begin(), entry.subscribers.end(), [](const registry_subscriber<Channel> &s) { return s.sub_qos.wants_message_info; });
}

inline void bump_band_drop(band_drop_counters &c, detail::drop_cause cause) noexcept
{
    switch(cause)
    {
        case detail::drop_cause::drop_oldest:
            ++c.dropped_oldest;
            return;
        case detail::drop_cause::drop_newest:
            ++c.dropped_newest;
            return;
        case detail::drop_cause::blocked:
            ++c.blocked;
            return;
        default:
            return;
    }
}

inline std::size_t read_band_drop(const band_drop_counters &c, detail::drop_cause cause) noexcept
{
    switch(cause)
    {
        case detail::drop_cause::drop_oldest:
            return c.dropped_oldest;
        case detail::drop_cause::drop_newest:
            return c.dropped_newest;
        case detail::drop_cause::blocked:
            return c.blocked;
        default:
            return 0;
    }
}

template<typename Map, typename Channel>
void add_topic_subscriber(Map &topics, std::uint64_t topic_hash, std::string_view fqn, Channel &channel, std::string_view node_name, const subscriber_qos &qos,
                          std::optional<std::uint64_t> type_id)
{
    auto &entry = topics[topic_hash];
    if(entry.fqn.empty())
        entry.fqn = std::string{fqn};

    for(const auto &sub : entry.subscribers)
        if(&sub.channel.get() == &channel)
            return;

    const locality tier = tier_of(channel.remote_endpoint().scheme);
    entry.subscribers.push_back(registry_subscriber<Channel>{tier, std::string{node_name}, qos, type_id, channel});
    recompute_wants_info(entry);
}

template<typename Map, typename Channel>
void remove_topic_subscriber(Map &topics, std::uint64_t topic_hash, const Channel &channel)
{
    auto it = topics.find(topic_hash);
    if(it == topics.end())
        return;

    std::erase_if(it->second.subscribers, [&](const registry_subscriber<Channel> &s) { return &s.channel.get() == &channel; });
    recompute_wants_info(it->second);
}

template<typename Map, typename Channel>
void remove_peer_subscribers(Map &topics, const Channel &channel)
{
    for(auto &[hash, entry] : topics)
    {
        std::erase_if(entry.subscribers, [&](const registry_subscriber<Channel> &s) { return &s.channel.get() == &channel; });
        recompute_wants_info(entry);
    }
}

}

#endif
