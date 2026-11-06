#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_DIALER_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_DIALER_H

#include "plexus/freertos/detail/lwip_errors.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/stream/stream_socket.h"

#include "plexus/detail/compat.h"

#include <utility>
#include <system_error>

namespace plexus::freertos::detail {

template<plexus::stream::stream_socket Socket>
class lwip_dialer
{
public:
    using failed_cb = plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>;

    void on_failed(failed_cb cb)
    {
        m_on_failed_cb = std::move(cb);
    }

    void report(const plexus::io::endpoint &ep, plexus::io::io_error e)
    {
        if(m_on_failed_cb)
            m_on_failed_cb(ep, e);
    }

    template<typename Connected>
    void dial(const plexus::io::endpoint &ep, Connected &&on_connected)
    {
        Socket socket;
        if(std::error_code ec = socket.connect(ep); ec)
            return report(ep, map_error(ec));
        on_connected(std::move(socket));
    }

private:
    failed_cb m_on_failed_cb;
};

}

#endif
