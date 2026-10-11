#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_DETAIL_FD_DOORBELL_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_DETAIL_FD_DOORBELL_H

#include "plexus/shm/detail/notifier_bridge.h"

#include <asio/posix/stream_descriptor.hpp>
#include <asio/io_context.hpp>

#include <unistd.h>

#include <memory>
#include <cstdint>
#include <optional>
#include <system_error>
#include <utility>

namespace plexus::asio::shm::detail {

// A process-local read descriptor (an eventfd counter or a self-pipe read end) watched on the
// user's io_context. The doorbell owns the descriptor and asio only borrows it, so detach() leaves
// it open for a teardown step that must still run before the close.
class fd_doorbell
{
public:
    enum class kind : std::uint8_t
    {
        counter,
        bytes
    };

    fd_doorbell() noexcept
            : m_fd(-1)
            , m_kind(kind::counter)
            , m_descriptor()
    {
    }

    ~fd_doorbell()
    {
        close();
    }

    fd_doorbell(const fd_doorbell &)            = delete;
    fd_doorbell &operator=(const fd_doorbell &) = delete;
    fd_doorbell(fd_doorbell &&)                 = delete;
    fd_doorbell &operator=(fd_doorbell &&)      = delete;

    bool adopt(int fd, kind k) noexcept
    {
        m_fd   = fd;
        m_kind = k;
        return fd >= 0;
    }

    int native_handle() const noexcept
    {
        return m_fd;
    }

    bool attached() const noexcept
    {
        return m_descriptor.has_value();
    }

    void attach(::asio::io_context &io)
    {
        m_descriptor.emplace(io, m_fd);
    }

    template<typename Handler>
    void watch(Handler &&handler)
    {
        m_descriptor->async_wait(::asio::posix::stream_descriptor::wait_read, std::forward<Handler>(handler));
    }

    // One read empties an eventfd counter; a self-pipe yields up to 64 pending pokes per read.
    void drain() noexcept
    {
        if(m_kind == kind::counter)
        {
            std::uint64_t count = 0;
            (void)::read(m_fd, &count, sizeof(count));
            return;
        }
        unsigned char pokes[64];
        (void)::read(m_fd, pokes, sizeof(pokes));
    }

    void detach() noexcept
    {
        if(!m_descriptor)
            return;
        std::error_code ignore;
        m_descriptor->cancel(ignore);
        m_descriptor->release();
        m_descriptor.reset();
    }

    void close() noexcept
    {
        detach();
        if(m_fd >= 0)
            ::close(m_fd);
        m_fd = -1;
    }

private:
    int m_fd;
    kind m_kind;
    std::optional<::asio::posix::stream_descriptor> m_descriptor;
};

// Each doorbell read posts one drain and re-arms the watch. A completion that lands after disarm
// finds the doorbell detached and touches nothing.
template<typename Policy, typename Owner>
void relay_wakes(const std::shared_ptr<Owner> &owner, fd_doorbell &doorbell, ::plexus::shm::detail::notifier_bridge<Policy> &bridge)
{
    doorbell.watch(
            [owner, &doorbell, &bridge](const std::error_code &ec)
            {
                if(ec || !doorbell.attached())
                    return;
                doorbell.drain();
                ::plexus::shm::detail::post_drain(owner, bridge);
                relay_wakes(owner, doorbell, bridge);
            });
}

}

#endif
