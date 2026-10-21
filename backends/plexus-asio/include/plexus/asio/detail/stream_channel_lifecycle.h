#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_LIFECYCLE_H

#include <asio/post.hpp>

#include <utility>
#include <system_error>

namespace plexus::asio::detail {

// Guarded on an open socket so a not-yet-open TLS accept defers to its own start_read;
// best-effort (the ec is swallowed — a clamped/rejected size keeps the socket usable).
template<typename Ch>
void stream_apply_socket_options(Ch &c)
{
    if(!c.socket().is_open())
        return;
    std::error_code ec;
    Ch::traits_type::apply_socket_options(c.socket(), c.m_socket_options, ec);
}

template<typename Ch>
void stream_notify_closed(Ch &c)
{
    if(c.m_ops.settled())
        c.m_closed.notify([&c](auto closure) { ::asio::post(c.m_io, std::move(closure)); });
}

template<typename Ch>
void stream_shutdown_socket(Ch &c)
{
    auto &sock = c.socket();
    if(!sock.is_open())
        return;
    std::error_code ec;
    Ch::traits_type::shutdown(sock, ec);
    (void)sock.close(ec);
    c.m_open    = false;
    c.m_reading = false;
}

// Closing runs once and nothing begins after it. The count closes first, holding the inbound as one operation until
// its own waits settle, so on_closed is posted by whichever aborted completion of the teardown drains it last.
template<typename Ch, typename BeforeTeardown>
void stream_close_with(Ch &c, BeforeTeardown before_teardown)
{
    if(c.m_ops.closing())
        return;
    c.m_open = false;
    c.m_ops.begin();
    c.m_ops.close();
    before_teardown();
    c.m_inbound.pending().on_settled([&c] { c.m_ops.end(); });
    c.m_inbound.shutdown();
    stream_shutdown_socket(c);
    c.m_on_data_cb           = nullptr;
    c.m_on_error_cb          = nullptr;
    c.m_on_protocol_close_cb = nullptr;
}

}

#endif
