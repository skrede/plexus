#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/wire/close_cause.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/close_notification.h"
#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::inproc {

// In-process byte_channel over the inproc_bus. send()/close() enqueue to the partner and return immediately: the
// partner's handlers fire only once the executor's step-loop delivers the packet, so a handler may call back in.
template<typename Clock = std::chrono::steady_clock>
class inproc_channel
{
public:
    explicit inproc_channel(inproc_executor<Clock> &ex)
            : m_closed(false)
            , m_partner()
            , m_bus(ex.bus())
            , m_local(m_bus.register_channel(this))
            , m_local_key(m_bus.key_for(m_local))
            , m_partner_key(0)
            , m_exec(ex)
            , m_close_notification()
    {
    }

    inproc_channel(inproc_executor<Clock> &ex, std::error_code &)
            : inproc_channel(ex)
    {
    }

    ~inproc_channel()
    {
        m_bus.deregister_channel(this);
    }

    inproc_channel(const inproc_channel &)            = delete;
    inproc_channel &operator=(const inproc_channel &) = delete;
    inproc_channel(inproc_channel &&)                 = delete;
    inproc_channel &operator=(inproc_channel &&)      = delete;

    // Pairing is post-construction because the Policy constructs from the executor alone. Keys are never reused; an
    // endpoint the bus never minted resolves to 0, whose sends drop in deliver_one as an unmatched key.
    void connect_to(const io::endpoint &partner)
    {
        m_partner     = partner;
        m_partner_key = m_bus.key_for(partner);
    }

    const io::endpoint &local_endpoint() const noexcept
    {
        return m_local;
    }

    void send(std::span<const std::byte> data)
    {
        if(!m_closed)
            m_bus.enqueue(m_partner_key, data, m_local_key);
    }

    void send_object(const io::object_carrier &carrier)
    {
        if(!m_closed)
            m_bus.enqueue_object(m_partner_key, carrier);
    }

    void close()
    {
        if(m_closed)
            return;
        m_bus.enqueue_close(m_partner_key);
        enter_closed();
    }

    io::endpoint remote_endpoint() const
    {
        return m_partner;
    }

    void on_data(detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }
    void on_object(detail::move_only_function<void(const io::object_carrier &)> cb)
    {
        m_on_object_cb = std::move(cb);
    }
    void on_closed(detail::move_only_function<void()> cb)
    {
        m_close_notification.install(std::move(cb));
        notify_closed();
    }
    void on_error(detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }
    void on_drop(detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    // Reached only from inproc_bus::deliver_one() inside step(), never synchronously from a peer's send()/close().
    void deliver(std::span<const std::byte> data)
    {
        if(!m_closed && m_on_data_cb)
            m_on_data_cb(data);
    }

    // Transfers the bus's reference to the callback, which owns the release; a closed or handlerless channel releases.
    void deliver_object(const io::object_carrier &carrier)
    {
        if(!m_closed && m_on_object_cb)
            m_on_object_cb(carrier);
        else
            io::release(carrier);
    }

    // The partner's close is this channel's fail; its handler runs last, after the detach, as it may destroy the owner.
    void deliver_close()
    {
        if(m_closed)
            return;
        auto on_error = std::exchange(m_on_error_cb, nullptr);
        enter_closed();
        if(on_error)
            on_error(io::io_error::broken_pipe);
    }

    void report_unroutable()
    {
        if(!m_closed && m_on_drop_cb)
            m_on_drop_cb(io::detail::drop_event{.cause = io::detail::drop_cause::unroutable, .transport = io::locality::process});
    }

    // The bus never produces a protocol close; the deterministic rig drives one through this entry.
    void deliver_protocol_close(wire::close_cause cause)
    {
        if(m_closed)
            return;
        auto on_protocol_close = std::exchange(m_on_protocol_close_cb, nullptr);
        close();
        if(on_protocol_close)
            on_protocol_close(cause);
    }

private:
    bool m_closed;
    io::endpoint m_partner;
    inproc_bus<Clock> &m_bus;
    io::endpoint m_local;
    std::uint64_t m_local_key;
    std::uint64_t m_partner_key;
    inproc_executor<Clock> &m_exec;
    io::detail::close_notification m_close_notification;
    detail::move_only_function<void(io::io_error)> m_on_error_cb;
    detail::move_only_function<void(const io::object_carrier &)> m_on_object_cb;
    detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;
    detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;

    void enter_closed()
    {
        m_closed               = true;
        m_on_error_cb          = nullptr;
        m_on_object_cb         = nullptr;
        m_on_data_cb           = nullptr;
        m_on_drop_cb           = nullptr;
        m_on_protocol_close_cb = nullptr;
        notify_closed();
    }

    void notify_closed()
    {
        if(m_closed)
            m_close_notification.notify([this](auto notify) { m_exec.post(std::move(notify)); });
    }
};

}

#endif
