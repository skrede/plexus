#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIPTION_ENDPOINT_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIPTION_ENDPOINT_H

#include "plexus/io/subscriber_registry.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"

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

    // True exactly on the 0->1 refcount transition (the one that puts a wire subscribe out).
    bool attach_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_registry.bump_refcount(node_name, fqn) == 1u;
    }

    // True exactly on the 1->0 transition (the one that puts a wire unsubscribe out).
    bool detach_gate(std::string_view node_name, std::string_view fqn)
    {
        return m_registry.drop_refcount(node_name, fqn) == 0u;
    }

    void remove_peer(const peer &p)
    {
        m_registry.remove_peer(p.node_name, p.channel);
    }

    void send_subscribe(channel_type &channel, const wire::subscribe_request &req)
    {
        auto bytes = wire::encode_subscribe_request(req);
        send_control(channel, wire::msg_type::subscribe, bytes);
    }

    // session_id = 0 on every control frame. Reuses a member scratch to stay allocation-light.
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        wire::frame_header fhdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = inner.size()};
        wire::encode_frame_into(m_control_scratch, fhdr, inner);
        channel.send(m_control_scratch);
    }

private:
    subscriber_registry<channel_type> m_registry;
    std::vector<std::byte> m_control_scratch;
    std::uint64_t m_next_sequence{0};
};

}

#endif
