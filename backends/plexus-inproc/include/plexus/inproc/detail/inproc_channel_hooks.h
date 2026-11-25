#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_CHANNEL_HOOKS_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_CHANNEL_HOOKS_H

#include "plexus/wire/close_cause.h"

#include "plexus/io/io_error.h"
#include "plexus/io/object_carrier.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>

// In plexus::detail beside dispatch_packet, for the same reason: a plexus::inproc::detail would shadow the bus's bare
// detail::move_only_function lookups.
namespace plexus::detail {

class inproc_channel_hooks
{
public:
    void on_data(move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }

    void on_object(move_only_function<void(const io::object_carrier &)> cb)
    {
        m_on_object_cb = std::move(cb);
    }

    void on_error(move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void on_drop(move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    void on_protocol_close(move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

protected:
    move_only_function<void(io::io_error)> m_on_error_cb;
    move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    move_only_function<void(const io::object_carrier &)> m_on_object_cb;
    move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;

    inproc_channel_hooks()
            : m_on_error_cb()
            , m_on_data_cb()
            , m_on_protocol_close_cb()
            , m_on_object_cb()
            , m_on_drop_cb()
    {
    }

    void clear_hooks() noexcept
    {
        m_on_error_cb          = nullptr;
        m_on_object_cb         = nullptr;
        m_on_data_cb           = nullptr;
        m_on_drop_cb           = nullptr;
        m_on_protocol_close_cb = nullptr;
    }
};

}

#endif
