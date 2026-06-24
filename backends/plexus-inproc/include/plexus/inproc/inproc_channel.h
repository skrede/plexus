#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/wire/close_cause.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::inproc {

// In-process byte_channel over the inproc_bus. send()/close() enqueue to the partner and return
// immediately — the partner's on_data/close does not fire until the executor's step-loop delivers
// the packet, so a handler can call back into the channel with no re-entrancy hazard.
template<typename Clock = std::chrono::steady_clock>
class inproc_channel
{
public:
    explicit inproc_channel(inproc_executor<Clock> &ex)
            : m_exec(&ex)
            , m_bus(&ex.bus())
            , m_local(m_bus->register_channel(this))
    {
    }

    inproc_channel(inproc_executor<Clock> &ex, std::error_code &)
            : inproc_channel(ex)
    {
    }

    ~inproc_channel()
    {
        if(m_bus)
            m_bus->deregister_channel(this);
    }

    inproc_channel(const inproc_channel &)            = delete;
    inproc_channel &operator=(const inproc_channel &) = delete;
    inproc_channel(inproc_channel &&)                 = delete;
    inproc_channel &operator=(inproc_channel &&)      = delete;

    // Pairing is post-construction because the Policy constructs from the executor alone. The
    // partner's bus key is resolved here once (keys are never reused); a partner endpoint the bus
    // never minted resolves to 0, whose sends drop in deliver_one as an unmatched key.
    void connect_to(const io::endpoint &partner)
    {
        m_partner     = partner;
        m_partner_key = m_bus ? m_bus->key_for(partner) : 0;
    }

    const io::endpoint &local_endpoint() const noexcept
    {
        return m_local;
    }

    void send(std::span<const std::byte> data)
    {
        if(m_bus && !m_closed)
            m_bus->enqueue(m_partner_key, data, this);
    }

    // A send on a closed channel does NOT enqueue and therefore does NOT addref, so no release is
    // owed for it.
    void send_object(const io::object_carrier &carrier)
    {
        if(m_bus && !m_closed)
            m_bus->enqueue_object(m_partner_key, carrier);
    }

    void close()
    {
        if(m_closed)
            return;
        m_closed = true;
        if(m_bus)
            m_bus->enqueue_close(m_partner_key);
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
        m_on_closed_cb = std::move(cb);
    }
    void on_error(detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }
    void on_drop(detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    // inproc moves whole pre-framed packets, so no partial frame can exist on it: the callback is
    // stored to satisfy the uniform channel seam and is NEVER fired here.
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    // The deliver_* entries are reached only from inproc_bus::deliver_one() inside step(), never
    // synchronously from a peer's send()/close().
    void deliver(std::span<const std::byte> data)
    {
        if(!m_closed && m_on_data_cb)
            m_on_data_cb(data);
    }

    // Transfers the bus's reference TO the callback (which owns the matching release); a delivery to
    // a closed or handlerless channel releases here instead, so no path leaks.
    void deliver_object(const io::object_carrier &carrier)
    {
        if(!m_closed && m_on_object_cb)
            m_on_object_cb(carrier);
        else
            io::release(carrier);
    }

    void deliver_close()
    {
        if(m_closed)
            return;
        if(m_on_closed_cb)
            m_on_closed_cb();
        if(m_on_error_cb)
            m_on_error_cb(io::io_error::broken_pipe);
    }

    // The bus calls this from deliver_one inside the step-loop, so the report is already off the
    // synchronous send() path — the posted drop contract.
    void report_unroutable()
    {
        if(m_on_drop_cb)
            m_on_drop_cb(io::detail::drop_event{.cause = io::detail::drop_cause::unroutable, .transport = io::locality::process});
    }

    // Exposed so the deterministic rig drives the on_protocol_close misbehavior path the bus cannot
    // produce.
    void deliver_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close_cb)
            m_on_protocol_close_cb(cause);
    }

private:
    inproc_executor<Clock> *m_exec;
    inproc_bus<Clock> *m_bus;
    io::endpoint m_local;
    io::endpoint m_partner;
    std::uint64_t m_partner_key{0};
    detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    detail::move_only_function<void(const io::object_carrier &)> m_on_object_cb;
    detail::move_only_function<void()> m_on_closed_cb;
    detail::move_only_function<void(io::io_error)> m_on_error_cb;
    detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;
    detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    bool m_closed{false};
};

}

#endif
