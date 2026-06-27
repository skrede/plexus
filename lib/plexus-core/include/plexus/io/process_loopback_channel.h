#ifndef HPP_GUARD_PLEXUS_IO_PROCESS_LOOPBACK_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_PROCESS_LOOPBACK_CHANNEL_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/object_carrier.h"
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
// object fast path (zero-copy by address) engages. close/on_closed/on_error/on_protocol_close are
// inert: a self-channel never drops.
template<typename Policy>
class process_loopback_channel
{
public:
    process_loopback_channel(typename Policy::executor_type executor, std::string self_name)
            : m_executor(executor)
            , m_self(endpoint{"inproc", std::move(self_name)})
    {
    }

    void send_object(const object_carrier &carrier)
    {
        detail::post_object<Policy>(m_executor, m_on_object_cb, carrier);
    }

    void send(std::span<const std::byte> bytes)
    {
        detail::post_bytes<Policy>(m_executor, m_on_data_cb, bytes);
    }

    endpoint remote_endpoint() const
    {
        return m_self;
    }

    void close()
    {
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
        m_on_closed_cb = std::move(cb);
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
    plexus::detail::move_only_function<void(const object_carrier &)> m_on_object_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void()> m_on_closed_cb;
    plexus::detail::move_only_function<void(io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
};

}

#endif
