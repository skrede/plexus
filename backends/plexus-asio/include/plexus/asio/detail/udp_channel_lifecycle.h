#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H

#include <asio/post.hpp>

namespace plexus::asio::detail {

// Posted, never synchronous: a this-capturing on_closed could otherwise run inline.
template<typename Ch>
void udp_post_on_closed(Ch &c)
{
    ::asio::post(c.m_io,
                 [&c]
                 {
                     if(c.m_on_closed_cb)
                         c.m_on_closed_cb();
                 });
}

template<typename Ch>
void udp_close(Ch &c)
{
    if(!c.m_open)
        return;
    c.m_open = false;
    udp_post_on_closed(c);
}

// Never posts on_closed (a this-capturing post could outlive the channel). The timers are canceled first so a timer
// firing after the channel dies is a canceled no-op, and the transport demux ref is erased before the channel dies.
template<typename Ch>
void udp_destroy(Ch &c)
{
    c.m_open = false;
    if(c.m_reassembler)
        c.m_reassembler->cancel();
    if(c.m_arq)
        c.m_arq->cancel();
    if(c.m_on_teardown_cb)
        c.m_on_teardown_cb();
}

}

#endif
