#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_LIFECYCLE_H

#include "plexus/io/io_error.h"
#include "plexus/io/detail/pending_ops.h"

#include <asio/post.hpp>

#include <memory>
#include <utility>

namespace plexus::asio::detail {

template<typename Ch>
void udp_notify_closed(Ch &c)
{
    if(c.m_ops.settled())
        c.m_closed.notify([&c](auto closure) { ::asio::post(c.m_io, std::move(closure)); });
}

// The dialer's handshake retransmit is a component of the channel it dials for, so it outlives every wait it issued;
// an accepted channel has none.
template<typename Ch>
typename Ch::handshake_type &udp_arm_handshake(Ch &c, typename Ch::handshake_type::schedule ladder)
{
    c.m_handshake = std::make_unique<typename Ch::handshake_type>(c.m_io, ladder);
    return *c.m_handshake;
}

template<typename Ch>
void udp_pair_handshake(Ch &c)
{
    if(c.m_handshake)
        c.m_handshake->on_paired_frame();
}

template<typename Component>
void udp_hold_until_settled(io::detail::pending_ops &ops, std::unique_ptr<Component> &component)
{
    if(!component)
        return;
    ops.begin();
    component->pending().on_settled([&ops] { ops.end(); });
}

template<typename Component>
void udp_cancel(std::unique_ptr<Component> &component)
{
    if(component)
        component->cancel();
}

template<typename Component>
void udp_cancel_unhooked(std::unique_ptr<Component> &component)
{
    if(!component)
        return;
    component->pending().on_settled({});
    component->cancel();
}

// Closing runs once and nothing begins after it. Each built component is held as one operation until its own waits
// settle, so on_closed is posted by whichever completion drains the count last.
template<typename Ch, typename BeforeTeardown>
void udp_close_with(Ch &c, BeforeTeardown before_teardown)
{
    if(c.m_ops.closing())
        return;
    c.m_open = false;
    udp_hold_until_settled(c.m_ops, c.m_arq);
    udp_hold_until_settled(c.m_ops, c.m_handshake);
    udp_hold_until_settled(c.m_ops, c.m_reassembler);
    c.m_ops.close();
    before_teardown();
    udp_cancel(c.m_arq);
    udp_cancel(c.m_handshake);
    udp_cancel(c.m_reassembler);
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
    udp_cancel_unhooked(c.m_arq);
    udp_cancel_unhooked(c.m_handshake);
    udp_cancel_unhooked(c.m_reassembler);
    if(auto leave_demux = std::exchange(c.m_on_teardown_cb, nullptr))
        leave_demux();
}

}

#endif
