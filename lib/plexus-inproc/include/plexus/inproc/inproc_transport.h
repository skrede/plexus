#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_TRANSPORT_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_TRANSPORT_H

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/detail/compat.h"

#include <chrono>
#include <memory>
#include <utility>

namespace plexus::inproc {

// The inproc connector: a bus-mediated rendezvous giving listen/dial the same
// shape asio's acceptor has, so the generic engine drives either backend
// uniformly. listen(ep) registers the accepting endpoint on the bus with a
// forwarder to this transport's on_accepted; dial(ep) looks the endpoint up,
// mints a connected channel pair, hands the accepted end to the listener and the
// dialer end to on_dialed. A dial to an unregistered endpoint fires
// on_dial_failed with connection_refused — symmetric with a refused TCP connect.
// The executor and bus are held by reference (no ownership); the caller owns the
// minted channels (peer_session borrows channel&). The Clock threads through so a
// deterministic reconnect oracle can drive it on a manual clock.
template <typename Clock = std::chrono::steady_clock>
class inproc_transport
{
public:
    inproc_transport(inproc_executor<Clock> &exec, inproc_bus<Clock> &bus)
        : m_exec(exec)
        , m_bus(bus)
    {
    }

    inproc_transport(const inproc_transport &) = delete;
    inproc_transport &operator=(const inproc_transport &) = delete;

    void on_accepted(detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(detail::move_only_function<void(io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep)
    {
        m_listen_ep = ep;
        m_bus.register_listener(ep, [this](std::unique_ptr<inproc_channel<Clock>> ch) {
            if(m_on_accepted)
                m_on_accepted(std::move(ch));
        });
    }

    void dial(const io::endpoint &ep)
    {
        auto *acc = m_bus.find_listener(ep);
        if(!acc)
            return report_dial_fail(io::io_error::connection_refused);
        auto dialer = std::make_unique<inproc_channel<Clock>>(m_exec);
        auto accepted = std::make_unique<inproc_channel<Clock>>(m_exec);
        dialer->connect_to(accepted->local_endpoint());
        accepted->connect_to(dialer->local_endpoint());
        if(acc->on_accepted)
            acc->on_accepted(std::move(accepted));
        if(m_on_dialed)
            m_on_dialed(std::move(dialer));
    }

    void close() { m_bus.deregister_listener(m_listen_ep); }

private:
    void report_dial_fail(io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(e);
    }

    inproc_executor<Clock> &m_exec;
    inproc_bus<Clock> &m_bus;
    io::endpoint m_listen_ep;
    detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> m_on_accepted;
    detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> m_on_dialed;
    detail::move_only_function<void(io::io_error)> m_on_dial_failed;
    detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::inproc::inproc_transport<>, plexus::inproc::inproc_policy>,
    "inproc_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
