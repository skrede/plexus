#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H

#include "plexus/io/io_error.h"

#include <asio/post.hpp>

#include <utility>

namespace plexus::asio::detail {

template<typename Ch>
void udp_notify_closed(Ch &c)
{
    if(c.m_ops.settled())
        c.m_closed.notify([&c](auto closure) { ::asio::post(c.m_io, std::move(closure)); });
}

// Closing runs once and nothing begins after it. The reliable ARQ is held as one operation until its own waits settle,
// so on_closed is posted by whichever completion drains the count last.
template<typename Ch, typename BeforeTeardown>
void udp_close_with(Ch &c, BeforeTeardown before_teardown)
{
    if(c.m_ops.closing())
        return;
    c.m_open = false;
    if(c.m_arq)
    {
        c.m_ops.begin();
        c.m_arq->pending().on_settled([&c] { c.m_ops.end(); });
    }
    c.m_ops.close();
    before_teardown();
    if(c.m_arq)
        c.m_arq->cancel();
    if(c.m_reassembler)
        c.m_reassembler->cancel();
    if(auto leave_demux = std::exchange(c.m_on_teardown_cb, nullptr))
        leave_demux();
    c.m_on_data_cb           = nullptr;
    c.m_on_error_cb          = nullptr;
    c.m_on_protocol_close_cb = nullptr;
    c.m_on_reliable_cb       = nullptr;
    c.m_on_drop_cb           = nullptr;
}

template<typename Ch>
void udp_fail(Ch &c, io::io_error e)
{
    udp_close_with(c,
                   [&c, e]
                   {
                       if(auto on_error = std::exchange(c.m_on_error_cb, nullptr))
                           on_error(e);
                   });
}

// A channel destroyed without a close still leaves the transport demux, so a later datagram is a miss.
template<typename Ch>
void udp_destroy(Ch &c)
{
    c.m_ops.on_settled({});
    if(c.m_arq)
    {
        c.m_arq->pending().on_settled({});
        c.m_arq->cancel();
    }
    if(c.m_reassembler)
        c.m_reassembler->cancel();
    if(auto leave_demux = std::exchange(c.m_on_teardown_cb, nullptr))
        leave_demux();
}

}

#endif
