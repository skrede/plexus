#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DELIVER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DELIVER_H

#include "plexus/io/detail/demand_table.h"

#include "plexus/io/message_info.h"

#include "plexus/node_id.h"
#include "plexus/publisher_gid.h"

#include "plexus/wire/data_frame.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

template<typename Forwarder>
void forwarder_drop(Forwarder &f, std::string_view message)
{
    f.m_logger.warn(message);
}

template<typename Forwarder>
void stamp_received(Forwarder &f, const node_id &source_node_id, std::uint64_t topic_hash)
{
    if(f.m_on_data_stamp_cb)
        f.m_on_data_stamp_cb(source_node_id, topic_hash);
}

enum class deliver_verdict : std::uint8_t
{
    decoded,
    undecodable,
    unsolicited,
    duplicate,
};

enum class sequence_gating : std::uint8_t
{
    ungated,
    gated,
};

struct deliver_outcome
{
    deliver_verdict verdict;
    std::uint64_t topic_hash;
};

// With demand_from set, data for a topic that peer was never asked for is refused before it is stamped or delivered, and
// a gated frame is refused when that peer's sequence gate for the topic has already admitted its sequence.
template<typename Endpoint>
deliver_verdict demand_verdict(Endpoint &endpoint, std::optional<std::string_view> demand_from, sequence_gating gating, std::string_view fqn, std::uint64_t sequence)
{
    if(!demand_from)
        return deliver_verdict::decoded;
    sequence_gate *gate = endpoint.demand_gate(*demand_from, fqn);
    if(!gate)
        return deliver_verdict::unsolicited;
    if(gating == sequence_gating::gated && !gate->admit(sequence))
        return deliver_verdict::duplicate;
    return deliver_verdict::decoded;
}

template<typename Forwarder, typename OnMessage>
deliver_outcome forwarder_deliver_checked(Forwarder &f, std::optional<std::string_view> demand_from, sequence_gating gating, std::span<const std::byte> inner, message_info info,
                                          const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
{
    auto decoded = wire::decode_unidirectional(inner, has_source_identity);
    if(!decoded)
        return {deliver_verdict::undecodable, 0};
    const std::uint64_t hash = decoded->header.topic_hash;
    auto fqn                 = f.m_endpoint.registry().fqn_for(hash);
    if(fqn.empty())
    {
        forwarder_drop(f, "plexus: forwarder topic_unknown_for_data");
        return {deliver_verdict::decoded, hash};
    }
    if(const deliver_verdict verdict = demand_verdict(f.m_endpoint, demand_from, gating, fqn, decoded->header.sequence); verdict != deliver_verdict::decoded)
        return {verdict, hash};
    info.publication_sequence = decoded->header.sequence;
    if(decoded->endpoint_counter)
        info.source_identity = publisher_gid{source_node_id, *decoded->endpoint_counter};

    stamp_received(f, source_node_id, hash);
    on_message(fqn, decoded->data, info);
    return {deliver_verdict::decoded, hash};
}

template<typename Forwarder, typename OnMessage>
bool forwarder_deliver(Forwarder &f, std::span<const std::byte> inner, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
{
    const auto sink               = [&on_message](std::string_view fqn, std::span<const std::byte> data, const message_info &) { on_message(fqn, data); };
    const deliver_outcome outcome = forwarder_deliver_checked(f, std::nullopt, sequence_gating::ungated, inner, message_info{}, source_node_id, has_source_identity, sink);
    return outcome.verdict != deliver_verdict::undecodable;
}

template<typename Forwarder, typename OnMessage>
bool forwarder_deliver(Forwarder &f, std::span<const std::byte> inner, message_info info, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
{
    return forwarder_deliver_checked(f, std::nullopt, sequence_gating::ungated, inner, info, source_node_id, has_source_identity, on_message).verdict != deliver_verdict::undecodable;
}

}

#endif
