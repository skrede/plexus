#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIPTION_ENDPOINT_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIPTION_ENDPOINT_H

#include "plexus/io/subscriber_registry.h"

#include "plexus/io/detail/demand_table.h"
#include "plexus/io/detail/keyed_refcount.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io {

template<typename Channel>
class subscription_endpoint
{
public:
    using channel_type = Channel;

    struct peer
    {
        channel_type &channel;
        std::string node_name;
    };

    subscriber_registry<channel_type> &registry() noexcept
    {
        return m_registry;
    }

    const subscriber_registry<channel_type> &registry() const noexcept
    {
        return m_registry;
    }

    std::uint64_t next_sequence() noexcept
    {
        return m_next_sequence++;
    }

    // The two directions never share state, so neither side's demand gates the other's subscribe,
    // answer or unsubscribe. The subscribe side holds one entry per demanded topic, the admit side counts.
    bool subscribe_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_subscribed.insert(node_name, fqn);
    }

    bool unsubscribe_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_subscribed.erase(node_name, fqn);
    }

    // Null when this node holds no demand for fqn from node_name.
    detail::sequence_gate *demand_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_subscribed.gate_for(node_name, fqn);
    }

    void arm_sequence_gate(std::string_view node_name, std::string_view fqn, std::uint64_t first_sequence)
    {
        if(detail::sequence_gate *gate = m_subscribed.gate_for(node_name, fqn))
            gate->arm(first_sequence);
    }

    bool admit_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_admitted.bump(node_name, fqn) == 1u;
    }

    bool release_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_admitted.drop(node_name, fqn) == 0u;
    }

    void remove_peer(const peer &p)
    {
        m_registry.remove_peer(p.channel);
        m_subscribed.forget(p.node_name);
        m_admitted.forget(p.node_name);
    }

    void send_subscribe(channel_type &channel, const wire::subscribe_request &req)
    {
        auto bytes = wire::encode_subscribe_request(req);
        send_control(channel, wire::msg_type::subscribe, bytes);
    }

    void send_declare(channel_type &channel, const wire::topic_declaration &td)
    {
        auto bytes = wire::encode_topic_declaration(td);
        send_control(channel, wire::msg_type::declare, bytes);
    }

    void send_peer_report(channel_type &channel, std::span<const std::byte> bytes)
    {
        send_control(channel, wire::msg_type::peer_report, bytes);
    }

    // Reuses a member scratch to stay allocation-light.
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        wire::frame_header fhdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = inner.size()};
        wire::encode_frame_into(m_control_scratch, fhdr, inner);
        channel.send(m_control_scratch);
    }

private:
    subscriber_registry<channel_type> m_registry;
    std::vector<std::byte> m_control_scratch;
    detail::demand_table m_subscribed;
    detail::keyed_refcount m_admitted;
    std::uint64_t m_next_sequence{0};
};

}

#endif
