#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_TRANSPORT_H

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_channel.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_serial_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <array>
#include <memory>
#include <cstddef>
#include <utility>
#include <system_error>
#include <string_view>

namespace plexus::asio {

// The host serial connector. A UART is point-to-point: there is no acceptor and serial_port has
// no async_connect (opening a port is a synchronous open(name) + set_option line discipline), so
// serial_transport implements transport_backend DIRECTLY — it does NOT derive from
// stream_transport (which wraps a Listener accept-loop + an async-connect dial). Both verbs
// collapse to open-port-and-read delivering ONE channel: listen(ep) via on_accepted (no endpoint
// — an inbound channel is not dial-correlated), dial(ep) via on_dialed carrying the endpoint (the
// engine's correlation key). On open failure dial reports on_dial_failed with a mapped io_error.
// By convention the dialing end drives the handshake request (on_outbound_connected) and the
// listening end takes the inbound bootstrap — the role is an endpoint config choice, no FSM
// change. close() is a no-op: the one delivered channel owns the open port and tears it down on
// destruction (there is no acceptor to stop).
class serial_transport
{
public:
    explicit serial_transport(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                              io::congestion        congestion = io::congestion::block,
                              io::egress_capacity   egress = io::egress_capacity::bounded_default(),
                              std::size_t global_default    = io::global_default_max_message_bytes,
                              std::size_t reassembly_budget = io::reassembly_memory_budget)
            : m_io(io)
            , m_cfg(wire::with_message_limits(cfg, global_default, reassembly_budget))
            , m_congestion(congestion)
            , m_egress(egress)
    {
    }

    serial_transport(const serial_transport &)            = delete;
    serial_transport &operator=(const serial_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<serial_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<serial_channel>,
                                                           const io::endpoint &)>
                           cb)
    {
        m_on_dialed = std::move(cb);
    }
    void
    on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    // A listen is an open-and-read with no acceptor: the one channel arrives via on_accepted. An
    // open failure surfaces on_error (a listen carries no endpoint to correlate a failure to).
    void listen(const io::endpoint &ep)
    {
        std::error_code ec;
        auto            ch = open_channel(ep, ec);
        if(ec)
            return report_error(detail::map_error(ec));
        if(m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // The dialed endpoint rides back through on_dialed / on_dial_failed so the engine correlates
    // the completion to its slot by endpoint.
    void dial(const io::endpoint &ep)
    {
        std::error_code ec;
        auto            ch = open_channel(ep, ec);
        if(ec)
            return report_dial_fail(ep, detail::map_error(ec));
        if(m_on_dialed)
            m_on_dialed(std::move(ch), ep);
    }

    // No acceptor to stop and no pending async dial to cancel — the delivered channel owns the
    // open port and closes it on teardown.
    void close() {}

    // The scheme this member serves + its locality tier: a directly-attached point-to-point UART
    // is the most-local link, serving the "serial" scheme.
    static constexpr std::array<std::string_view, 1> mux_schemes{"serial"};
    static constexpr io::transport_kind              mux_tier = io::transport_kind::local;

private:
    // Open the named device, apply the parsed baud + the default line discipline, and adopt the
    // configured port into a fresh serial_channel that immediately starts reading. The line
    // discipline is applied HERE (on the open port, before adoption) rather than through the
    // channel's socket-options hook so serial_traits::apply_socket_options stays a no-op.
    std::unique_ptr<serial_channel> open_channel(const io::endpoint &ep, std::error_code &ec)
    {
        const auto parsed = detail::parse_serial(ep.address, ec);
        if(ec)
            return nullptr;
        ::asio::serial_port port{m_io};
        port.open(parsed.device, ec);
        if(ec)
            return nullptr;
        apply_line_discipline(port, parsed.baud, ec);
        if(ec)
            return nullptr;
        auto ch = std::make_unique<serial_channel>(m_io, std::move(port), m_cfg, m_congestion,
                                                   m_egress);
        ch->start_read();
        return ch;
    }

    static void apply_line_discipline(::asio::serial_port &port, std::uint32_t baud,
                                      std::error_code &ec)
    {
        using base = ::asio::serial_port_base;
        port.set_option(base::baud_rate(baud), ec);
        if(ec)
            return;
        port.set_option(base::character_size(8), ec);
        if(ec)
            return;
        port.set_option(base::flow_control(base::flow_control::none), ec);
        if(ec)
            return;
        port.set_option(base::parity(base::parity::none), ec);
        if(ec)
            return;
        port.set_option(base::stop_bits(base::stop_bits::one), ec);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }
    void report_error(io::io_error e)
    {
        if(m_on_error)
            m_on_error(e);
    }

    ::asio::io_context         &m_io;
    wire::stream_inbound_config m_cfg;
    io::congestion              m_congestion;
    io::egress_capacity         m_egress;
    plexus::detail::move_only_function<void(std::unique_ptr<serial_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<serial_channel>, const io::endpoint &)>
                                                                                m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)>                       m_on_error;
};

}

static_assert(
        plexus::io::transport_backend<plexus::asio::serial_transport, plexus::asio::serial_policy>,
        "serial_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
