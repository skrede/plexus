#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_LIFECYCLE_H

#include <asio/post.hpp>

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

// on_closed on ONE discipline: posted, exactly once. The closed_fired latch collapses the
// close()/stream_fail double-edge to a single delivery; the closure captures the shared
// liveness block and checks alive, so a post still queued at destruction no-ops rather than
// dangling on this.
template<typename Ch>
void stream_post_on_closed(Ch &c)
{
    if(c.m_life->closed_fired)
        return;
    c.m_life->closed_fired = true;
    ::asio::post(c.m_io,
                 [&c, life = c.m_life]
                 {
                     if(!life->alive)
                         return;
                     if(c.m_on_closed_cb)
                         c.m_on_closed_cb();
                 });
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

}

#endif
