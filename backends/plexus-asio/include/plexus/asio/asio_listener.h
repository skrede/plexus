#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_LISTENER_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_LISTENER_H

#include "plexus/asio/asio_channel.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/detail/compat.h"

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <string>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace plexus::asio {

class asio_listener
{
public:
    explicit asio_listener(::asio::io_context &io, stream::stream_inbound_config cfg = {}, bool no_delay = true, io::congestion congestion = io::congestion::block,
                           io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options socket_options = {})
            : m_io(io)
            , m_acceptor(io)
            , m_cfg(cfg)
            , m_no_delay(no_delay)
            , m_congestion(congestion)
            , m_egress_capacity(egress)
            , m_socket_options(socket_options)
    {
    }

    asio_listener(const asio_listener &)            = delete;
    asio_listener &operator=(const asio_listener &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void start(const io::endpoint &bind_ep)
    {
        std::error_code ec;
        auto ep = detail::parse(bind_ep.address, ec);
        if(ec)
            return report(ec);
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return report(ec);
        (void)m_acceptor.set_option(::asio::socket_base::reuse_address(true), ec);
        m_acceptor.bind(ep, ec);
        if(ec)
            return report(ec);
        m_acceptor.listen(::asio::socket_base::max_listen_connections, ec);
        if(ec)
            return report(ec);
        m_running = true;
        do_accept();
    }

    void stop()
    {
        m_running = false;
        std::error_code ec;
        (void)m_acceptor.cancel(ec);
        (void)m_acceptor.close(ec);
    }

    uint16_t port() const
    {
        std::error_code ec;
        auto ep = m_acceptor.local_endpoint(ec);
        return ec ? 0u : ep.port();
    }

private:
    void do_accept()
    {
        m_acceptor.async_accept(
                [this](std::error_code ec, ::asio::ip::tcp::socket peer)
                {
                    if(ec)
                    {
                        if(ec != ::asio::error::operation_aborted)
                            report(ec);
                        return;
                    }
                    if(m_no_delay)
                    {
                        // Disable Nagle BEFORE the channel adopts the socket — the accept ctor
                        // starts reading immediately.
                        std::error_code nec;
                        (void)peer.set_option(::asio::ip::tcp::no_delay(true), nec);
                    }
                    auto channel = std::make_unique<asio_channel>(m_io, std::move(peer), m_cfg, m_congestion, m_egress_capacity, m_socket_options);
                    if(m_on_accepted)
                        m_on_accepted(std::move(channel));
                    if(m_running)
                        do_accept();
                });
    }

    void report(const std::error_code &ec)
    {
        if(m_on_error)
            m_on_error(detail::map_error(ec));
    }

    ::asio::io_context &m_io;
    ::asio::ip::tcp::acceptor m_acceptor;
    stream::stream_inbound_config m_cfg;
    bool m_no_delay;
    io::congestion m_congestion;
    io::egress_capacity m_egress_capacity;
    stream_socket_options m_socket_options;
    plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_running{false};
};

}

#endif
