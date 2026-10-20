#ifndef HPP_GUARD_PLEXUS_IO_PROCESS_LOOPBACK_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_PROCESS_LOOPBACK_CHANNEL_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/object_carrier.h"

#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/close_notification.h"
#include "plexus/io/detail/process_loopback_post.h"

#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <utility>

namespace plexus::io {

// The pre-connected self-channel a node attaches to its OWN forwarder: send/send_object never fire
// their hook synchronously — each posts re-entry onto the node's executor (the byte_channel posted
// contract), so a subscriber re-publishing from its callback cannot re-enter the fan-out mid-loop.
// remote_endpoint reports scheme "inproc" so the forwarder classifies it process tier and the
// object fast path (zero-copy by address) engages. Each post reaches the channel's handlers by
// reference and is counted, so on_closed waits for the last of them. A self-channel never drops:
// on_error and on_protocol_close never fire, and only its owner's close() closes it.
template<typename Policy>
class process_loopback_channel
{
public:
    process_loopback_channel(typename Policy::executor_type executor, std::string self_name)
            : m_executor(executor)
            , m_self(endpoint{"inproc", std::move(self_name)})
            , m_ops()
            , m_close_notification()
    {
        m_ops.on_settled([this] { notify_closed(); });
    }

    process_loopback_channel(const process_loopback_channel &)            = delete;
    process_loopback_channel &operator=(const process_loopback_channel &) = delete;

    void send_object(const object_carrier &carrier)
    {
        if(!m_ops.closing())
            detail::post_object<Policy>(m_executor, m_ops, m_on_object_cb, carrier);
    }

    void send(std::span<const std::byte> bytes)
    {
        if(!m_ops.closing())
            detail::post_bytes<Policy>(m_executor, m_ops, m_on_data_cb, bytes);
    }

    endpoint remote_endpoint() const
    {
        return m_self;
    }

    void close()
    {
        if(m_ops.closing())
            return;
        m_on_object_cb         = nullptr;
        m_on_data_cb           = nullptr;
        m_on_error_cb          = nullptr;
        m_on_protocol_close_cb = nullptr;
        m_ops.close();
    }

    void on_object(plexus::detail::move_only_function<void(const object_carrier &)> cb)
    {
        m_on_object_cb = std::move(cb);
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close_notification.install(std::move(cb));
        notify_closed();
    }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

private:
    typename Policy::executor_type m_executor;
    endpoint m_self;
    detail::pending_ops m_ops;
    detail::close_notification m_close_notification;
    plexus::detail::move_only_function<void(const object_carrier &)> m_on_object_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void(io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;

    void notify_closed()
    {
        if(m_ops.settled())
            m_close_notification.notify([this](auto notify) { Policy::post(m_executor, std::move(notify)); });
    }
};
}

#endif
