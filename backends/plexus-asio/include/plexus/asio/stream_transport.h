#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_TRANSPORT_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>

#include <memory>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio {

// The shared connector core for the plaintext stream backends (TCP, AF_UNIX): a Listener for listen + accept plus an
// async-connect dial. A dial sits in the pending table from before its connect, which it counts as an operation of the
// channel, so close() retires a dial still connecting; the owner closes the transport and drains before destroying it.
template<typename Channel, typename Listener, typename Traits>
class stream_transport
{
public:
    using channel_type = Channel;

    stream_transport(const stream_transport &)            = delete;
    stream_transport &operator=(const stream_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<Channel>)> cb)
    {
        m_on_accepted_cb = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<Channel>, const io::endpoint &)> cb)
    {
        m_on_dialed_cb = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void listen(const io::endpoint &ep)
    {
        m_listener.start(ep);
    }

    // The dialed endpoint rides the async closure so on_dialed / on_dial_failed carry it back:
    // the engine correlates each completion to its slot by endpoint, not by arrival order
    // (concurrent dials over one transport reorder).
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto target = Traits::parse(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, detail::map_error(pec));
        auto ch   = std::make_unique<Channel>(m_io, m_cfg, m_congestion, m_egress_capacity, m_socket_options);
        auto *raw = ch.get();
        m_pending.insert(raw, std::move(ch), ep);
        raw->begin_op();
        raw->socket().async_connect(target, [this, ep, raw](std::error_code ec) { finish_dial(ep, raw, ec); });
    }

    void close()
    {
        m_pending.retire_all(m_on_dial_failed_cb);
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
            , m_pending()
    {
        m_listener.on_accepted([this](std::unique_ptr<Channel> ch) { m_pending.deliver_or_retire(m_on_accepted_cb, std::move(ch)); });
        m_listener.on_error(
                [this](io::io_error e)
                {
                    if(m_on_error_cb)
                        m_on_error_cb(e);
                });
    }

    Listener &listener() noexcept
    {
        return m_listener;
    }
    const Listener &listener() const noexcept
    {
        return m_listener;
    }

private:
    // A dial close() already retired is reported by the registry when its channel settles, so its own completion stays silent.
    void finish_dial(const io::endpoint &ep, Channel *raw, std::error_code ec)
    {
        raw->end_op();
        if(!m_pending.is_pending(raw))
            return;
        if(ec)
        {
            m_pending.fail(raw);
            return report_dial_fail(ep, detail::map_error(ec));
        }
        auto ch = m_pending.resolve(raw);
        Traits::after_connect(*ch, m_no_delay);
        ch->start_read();
        m_pending.deliver_or_retire(m_on_dialed_cb, std::move(ch), ep);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed_cb)
            m_on_dial_failed_cb(ep, e);
    }

    ::asio::io_context &m_io;
    Listener m_listener;
    stream::stream_inbound_config m_cfg;
    bool m_no_delay;
    io::congestion m_congestion;
    io::egress_capacity m_egress_capacity;
    stream_socket_options m_socket_options;
    io::pending_dial_registry<Channel> m_pending;
    plexus::detail::move_only_function<void(std::unique_ptr<Channel>)> m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<Channel>, const io::endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed_cb;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
};

}

#endif
