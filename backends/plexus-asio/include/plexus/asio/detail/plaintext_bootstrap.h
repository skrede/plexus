#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_PLAINTEXT_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_PLAINTEXT_BOOTSTRAP_H

#include "plexus/wire_bytes.h"

#include <span>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

template<typename Stream>
class plaintext_bootstrap
{
public:
    template<typename Io>
    static Stream make_stream(Io &io)
    {
        return Stream{io};
    }

    template<typename Io, typename Socket>
    static Stream make_stream(Io &io, Socket connected)
    {
        (void)io;
        return Stream{std::move(connected)};
    }

    template<typename Channel>
    void submit(Channel &c, std::span<const std::byte> data)
    {
        c.enqueue_egress(data);
    }

    template<typename Channel>
    void submit(Channel &c, plexus::wire_bytes<> data)
    {
        c.enqueue_egress_owned(std::move(data));
    }

    template<typename Channel>
    void arm_on_accept(Channel &c)
    {
        c.mark_open();
        c.start_read_loop();
    }

    void reset() noexcept
    {
    }
};

}

#endif
