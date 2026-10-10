#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DELIVER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DELIVER_H

#include "plexus/io/message_info.h"

#include "plexus/node_id.h"
#include "plexus/publisher_gid.h"

#include "plexus/wire/data_frame.h"

#include <span>
#include <cstddef>
#include <cstdint>
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

template<typename Forwarder, typename OnMessage>
void forwarder_deliver(Forwarder &f, std::span<const std::byte> inner, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
{
    auto decoded = wire::decode_unidirectional(inner, has_source_identity);
    if(!decoded)
        return forwarder_drop(f, "plexus: forwarder unidirectional_decode_failed");

    auto fqn = f.m_endpoint.registry().fqn_for(decoded->header.topic_hash);
    if(fqn.empty())
        return forwarder_drop(f, "plexus: forwarder topic_unknown_for_data");

    stamp_received(f, source_node_id, decoded->header.topic_hash);
    on_message(fqn, decoded->data);
}

template<typename Forwarder, typename OnMessage>
void forwarder_deliver(Forwarder &f, std::span<const std::byte> inner, message_info info, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
{
    auto decoded = wire::decode_unidirectional(inner, has_source_identity);
    if(!decoded)
        return forwarder_drop(f, "plexus: forwarder unidirectional_decode_failed");

    auto fqn = f.m_endpoint.registry().fqn_for(decoded->header.topic_hash);
    if(fqn.empty())
        return forwarder_drop(f, "plexus: forwarder topic_unknown_for_data");

    info.publication_sequence = decoded->header.sequence;
    if(decoded->endpoint_counter)
        info.source_identity = publisher_gid{source_node_id, *decoded->endpoint_counter};

    stamp_received(f, source_node_id, decoded->header.topic_hash);
    on_message(fqn, decoded->data, info);
}

}

#endif
