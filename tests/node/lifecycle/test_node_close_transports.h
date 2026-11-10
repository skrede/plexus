#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_TRANSPORTS_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_TRANSPORTS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_selector.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <memory>
#include <utility>
#include <string_view>

namespace node_close_fixture {

using plexus::io::endpoint;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::detail::move_only_function;

// Every dial completes on a later executor turn, so a test can act on a slot whose dial is still in flight.
class deferred_dial_transport
{
public:
    deferred_dial_transport(inproc_executor<> &ex, inproc_bus<> &bus)
            : m_ex(ex)
            , m_inner(ex, bus)
    {
    }

    void listen(const endpoint &ep)
    {
        m_inner.listen(ep);
    }

    void dial(const endpoint &ep)
    {
        m_ex.post([this, ep] { m_inner.dial(ep); });
    }

    void close()
    {
        m_inner.close();
    }

    void on_accepted(move_only_function<void(std::unique_ptr<inproc_channel<>>)> cb)
    {
        m_inner.on_accepted(std::move(cb));
    }

    void on_dialed(move_only_function<void(std::unique_ptr<inproc_channel<>>, const endpoint &)> cb)
    {
        m_inner.on_dialed(std::move(cb));
    }

    void on_dial_failed(move_only_function<void(const endpoint &, plexus::io::io_error)> cb)
    {
        m_inner.on_dial_failed(std::move(cb));
    }

    void on_error(move_only_function<void(plexus::io::io_error)> cb)
    {
        m_inner.on_error(std::move(cb));
    }

private:
    inproc_executor<> &m_ex;
    inproc_transport<> m_inner;
};

// A mux member that only records which of its completion callbacks are installed.
class recording_member
{
public:
    using channel_type = inproc_channel<>;

    static constexpr std::array<std::string_view, 1> k_schemes{"tcp"};
    static constexpr std::span<const std::string_view> mux_schemes{k_schemes};
    static constexpr plexus::io::transport_kind mux_tier = plexus::io::transport_kind::remote;

    recording_member()
            : m_accepted_cb()
            , m_dialed_cb()
            , m_dial_failed_cb()
            , m_error_cb()
    {
    }

    void listen(const endpoint &)
    {
    }

    void dial(const endpoint &)
    {
    }

    void close()
    {
    }

    void on_accepted(move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_accepted_cb = std::move(cb);
    }

    void on_dialed(move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> cb)
    {
        m_dialed_cb = std::move(cb);
    }

    void on_dial_failed(move_only_function<void(const endpoint &, plexus::io::io_error)> cb)
    {
        m_dial_failed_cb = std::move(cb);
    }

    void on_error(move_only_function<void(plexus::io::io_error)> cb)
    {
        m_error_cb = std::move(cb);
    }

    bool any_installed() const noexcept
    {
        return m_accepted_cb || m_dialed_cb || m_dial_failed_cb || m_error_cb;
    }

private:
    move_only_function<void(std::unique_ptr<channel_type>)> m_accepted_cb;
    move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> m_dialed_cb;
    move_only_function<void(const endpoint &, plexus::io::io_error)> m_dial_failed_cb;
    move_only_function<void(plexus::io::io_error)> m_error_cb;
};

}

#endif
