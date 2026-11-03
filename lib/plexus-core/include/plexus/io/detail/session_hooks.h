#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_SESSION_HOOKS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_SESSION_HOOKS_H

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"

#include "plexus/wire/subscribe.h"

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::io::detail {

class session_hooks
{
public:
    template<typename OnMessage>
    void on_message(OnMessage on_message)
    {
        m_on_message_cb = std::move(on_message);
    }

    template<typename OnMessageWithInfo>
    void on_message_with_info(OnMessageWithInfo cb)
    {
        m_on_message_with_info_cb = std::move(cb);
    }

    void on_message_route(plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> cb)
    {
        m_on_message_route_cb = std::move(cb);
    }

    void on_object_route(plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> cb)
    {
        m_on_object_route_cb = std::move(cb);
    }

    void on_transport_drop(plexus::detail::move_only_function<void()> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    void on_lifecycle(plexus::detail::move_only_function<void(const lifecycle_event &)> cb)
    {
        m_on_lifecycle_cb = std::move(cb);
    }

    void on_stamp_seen(plexus::detail::move_only_function<void(const node_id &)> cb)
    {
        m_on_stamp_seen_cb = std::move(cb);
    }

    void on_decline_seen(plexus::detail::move_only_function<void(const node_id &, bool)> cb)
    {
        m_on_decline_seen_cb = std::move(cb);
    }

    void on_security(plexus::detail::move_only_function<void(const security_event &)> cb)
    {
        m_on_security_cb = std::move(cb);
    }

    void on_subscribe_refused(plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> cb)
    {
        m_on_subscribe_refused_cb = std::move(cb);
    }

    void on_subscribe_degraded(plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> cb)
    {
        m_on_subscribe_degraded_cb = std::move(cb);
    }

protected:
    // Protected so the detail free functions reach them through the friend grants of the session deriving from this class.
    plexus::detail::move_only_function<void()> m_on_drop_cb;
    plexus::detail::move_only_function<void(const node_id &)> m_on_stamp_seen_cb;
    plexus::detail::move_only_function<void(const security_event &)> m_on_security_cb;
    plexus::detail::move_only_function<void(const lifecycle_event &)> m_on_lifecycle_cb;
    plexus::detail::move_only_function<void(const node_id &, bool)> m_on_decline_seen_cb;
    plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> m_on_subscribe_degraded_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>)> m_on_message_cb;
    plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> m_on_object_route_cb;
    plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> m_on_subscribe_refused_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_route_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_with_info_cb;
};

}

#endif
