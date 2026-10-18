#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_SUBSCRIBE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_SUBSCRIBE_H

#include "plexus/io/subscriber_qos.h"
#include "plexus/io/subscribe_qos_wire.h"

#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_declaration.h"

#include <string>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

// A named type is declared; an id with no name is a type declared without one; neither is no
// assertion at all. The wire's three states and the subscribe flag both read this one rule.
inline wire::type_state state_of(std::optional<std::uint64_t> type_id, std::string_view type_name)
{
    if(!type_name.empty())
        return wire::type_state::declared;
    return type_id ? wire::type_state::declared_empty : wire::type_state::undeclared;
}

template<typename Forwarder, typename Channel>
void send_subscribe(Forwarder &f, Channel &channel, std::string_view fqn, std::uint64_t hash, std::optional<std::uint64_t> type_id, const subscriber_qos &sub_qos,
                    std::string_view type_name)
{
    wire::subscribe_request req{.fqn           = std::string{fqn},
                                .type_name     = std::string{type_name},
                                .type_declared = state_of(type_id, type_name) != wire::type_state::undeclared,
                                .topic_hash    = hash,
                                .type_hash     = type_id.value_or(0), // 0 = undeclared
                                .source        = wire::endpoint_source_type::publisher};

    if(!(sub_qos == subscriber_qos{}))
    {
        req.has_qos = true;
        req.qos     = to_wire_region(sub_qos);
    }
    f.m_endpoint.send_subscribe(channel, req);
}

}

#endif
