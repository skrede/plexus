#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_DETAIL_FD_DOORBELL_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_DETAIL_FD_DOORBELL_H

#include "plexus/shm/detail/notifier_bridge.h"

#include <asio/posix/stream_descriptor.hpp>
#include <asio/io_context.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <cstdint>
#include <utility>
#include <optional>
#include <system_error>

namespace plexus::asio::shm::detail {

// A process-local read descriptor (an eventfd counter or a self-pipe read end) watched on the
// user's io_context. The doorbell owns the descriptor and asio only borrows it, so detach() leaves
// it open for a teardown step that must still run before the close. Pokes are coalesced: a poke
// writes only when none is pending since the reactor last drained, so a stalled reactor never fills
// the descriptor and a poke never blocks.
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
            , m_write(-1)
            , m_pending(false)
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

    bool open_pipe() noexcept
    {
        int fds[2] = {-1, -1};
        if(::pipe(fds) != 0)
            return false;
        for(const int fd : fds)
        {
            ::fcntl(fd, F_SETFL, O_NONBLOCK);
            ::fcntl(fd, F_SETFD, FD_CLOEXEC);
        }
        m_write = fds[1];
        return adopt(fds[0], kind::bytes);
    }

    // A full descriptor (EAGAIN) already holds a pending poke, so the failed write is dropped.
    void poke() noexcept
    {
        if(m_pending.exchange(true, std::memory_order_acq_rel))
            return;
        if(m_kind == kind::counter)
        {
            const std::uint64_t one = 1;
            (void)::write(m_fd, &one, sizeof(one));
            return;
        }
        const unsigned char one = 1;
        (void)::write(m_write, &one, sizeof(one));
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

    // The pending flag clears after the read and before the caller posts its drain: a poke that still found it set is
    // covered by that drain, and a poke after the clear writes again and re-fires the watch.
    void drain() noexcept
    {
        if(m_kind == kind::counter)
        {
            std::uint64_t count = 0;
            (void)::read(m_fd, &count, sizeof(count));
        }
        else
        {
            unsigned char pokes[64];
            (void)::read(m_fd, pokes, sizeof(pokes));
        }
        m_pending.exchange(false, std::memory_order_acq_rel);
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
        if(m_write >= 0)
            ::close(m_write);
        m_fd    = -1;
        m_write = -1;
        m_pending.store(false, std::memory_order_release);
    }

private:
    int m_fd;
    kind m_kind;
    int m_write;
    std::atomic<bool> m_pending;
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
