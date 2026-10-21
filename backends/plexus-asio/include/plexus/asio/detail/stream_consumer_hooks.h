#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CONSUMER_HOOKS_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CONSUMER_HOOKS_H

#include "plexus/wire/close_cause.h"

#include "plexus/io/io_error.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>

namespace plexus::asio::detail {

class stream_consumer_hooks
{
public:
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

protected:
    stream_consumer_hooks()
            : m_on_error_cb()
            , m_on_protocol_close_cb()
            , m_on_data_cb()
    {
    }

    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
};

}

#endif
