#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_TRANSPORT_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>

#include <memory>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio {

// The shared connector core for the plaintext stream backends (TCP, AF_UNIX): wraps a
// Listener for listen + accept and adds an async-connect dial. listen(ep) starts the
// acceptor and forwards each accepted channel through on_accepted; dial(ep) parses the
// target via Traits, async-connects a fresh client channel, applies the Traits' post-connect
// step (TCP disables Nagle; AF_UNIX is a no-op — AF_UNIX has no Nagle), starts the read loop,
// and hands the live channel to on_dialed (on failure fires on_dial_failed with a mapped
// io_error). The connect closure OWNS the channel for the duration of the async op (the
// move_only_function holds the unique_ptr) — no shared lifetime handle is taken; the
// transport owner must outlive any pending connect, the same caller-owned lifetime discipline
// the channel and listener document. close() stops the acceptor.
template<typename Channel, typename Listener, typename Traits>
class stream_transport
{
public:
    using channel_type = Channel;

    stream_transport(const stream_transport &)            = delete;
    stream_transport &operator=(const stream_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<Channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<Channel>, const io::endpoint &)> cb)
    {
        m_on_dialed = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const io::endpoint &ep)
    {
        m_listener.start(ep);
    }

    // The dialed endpoint rides the async closure so on_dialed / on_dial_failed CARRY it back:
    // the engine correlates each completion to its slot by endpoint, not by arrival order
    // (concurrent dials over one transport reorder).
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            target = Traits::parse(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, detail::map_error(pec));
        auto  ch  = std::make_unique<Channel>(m_io, m_cfg, m_congestion, m_egress_capacity, m_socket_options);
        auto &raw = *ch;
        raw.socket().async_connect(target,
                                   [this, ep, ch = std::move(ch)](std::error_code ec) mutable
                                   {
                                       if(ec)
                                           return report_dial_fail(ep, detail::map_error(ec));
                                       Traits::after_connect(*ch, m_no_delay);
                                       ch->start_read();
                                       if(m_on_dialed)
                                           m_on_dialed(std::move(ch), ep);
                                   });
    }

    void close()
    {
        m_listener.stop();
    }

protected:
    template<typename... ListenerArgs>
    stream_transport(::asio::io_context &io, stream::stream_inbound_config cfg, bool no_delay, io::congestion congestion, io::egress_capacity egress,
                     stream_socket_options socket_options, ListenerArgs &&...largs)
            : m_io(io)
            , m_listener(std::forward<ListenerArgs>(largs)...)
            , m_cfg(cfg)
            , m_no_delay(no_delay)
            , m_congestion(congestion)
            , m_egress_capacity(egress)
            , m_socket_options(socket_options)
    {
        m_listener.on_accepted(
                [this](std::unique_ptr<Channel> ch)
                {
                    if(m_on_accepted)
                        m_on_accepted(std::move(ch));
                });
        m_listener.on_error(
                [this](io::io_error e)
                {
                    if(m_on_error)
                        m_on_error(e);
                });
    }

    [[nodiscard]] Listener &listener() noexcept
    {
        return m_listener;
    }
    [[nodiscard]] const Listener &listener() const noexcept
    {
        return m_listener;
    }

private:
    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    ::asio::io_context                                                                      &m_io;
    Listener                                                                                 m_listener;
    stream::stream_inbound_config                                                            m_cfg;
    bool                                                                                     m_no_delay;
    io::congestion                                                                           m_congestion;
    io::egress_capacity                                                                      m_egress_capacity;
    stream_socket_options                                                                    m_socket_options;
    plexus::detail::move_only_function<void(std::unique_ptr<Channel>)>                       m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<Channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)>             m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)>                                   m_on_error;
};

}

#endif
