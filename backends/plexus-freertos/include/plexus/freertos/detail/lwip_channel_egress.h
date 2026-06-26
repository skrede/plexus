#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_EGRESS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_EGRESS_H

#include "plexus/stream/stream_socket.h"
#include "plexus/stream/detail/send_queue.h"

#include <span>
#include <cstddef>
#include <utility>

namespace plexus::freertos::detail {

// The egress/drain half of an lwip_channel: a bounded send_queue over a borrowed connected
// stream_socket. lwIP has no scatter writev, so the gathered views are sent one by one; a transient
// short send is local congestion the socket already folded to 0 (the queue re-arms), never a
// tear-down. The socket is borrowed by reference — the owning channel outlives this helper.
template<plexus::stream::stream_socket S>
class lwip_channel_egress
{
public:
    lwip_channel_egress(S &socket, std::size_t egress_cap)
            : m_socket(socket)
            , m_send_queue([this](plexus::stream::detail::send_queue::buffer_sequence views, plexus::stream::detail::send_queue::completion done) { drain_views(views, std::move(done)); }, egress_cap)
    {
    }

    void send(std::span<const std::byte> framed)
    {
        m_send_queue.enqueue(framed);
    }

    void close()
    {
        m_send_queue.close_and_drain();
    }

private:
    void drain_views(plexus::stream::detail::send_queue::buffer_sequence views, plexus::stream::detail::send_queue::completion done)
    {
        for(const auto &v : views)
            m_socket.send(v);
        done(true);
    }

    S                                 &m_socket;
    plexus::stream::detail::send_queue m_send_queue;
};

}

#endif
