#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_LIFECYCLE_H

#include "plexus/freertos/detail/close_state.h"
#include "plexus/freertos/detail/lwip_rx_slot_pool.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/freertos/posted_work.h"
#include "plexus/freertos/freertos_executor.h"

#include <atomic>
#include <cstddef>

namespace plexus::freertos::detail {

template<typename Owner, typename Socket, std::size_t Slots, std::size_t Capacity>
class lwip_rx_lifecycle
{
public:
    using slot_type = rx_slot<Owner, Capacity>;

    // The RX hand-off pool holds k_slots buffers within the executor queue depth, so one channel never holds more
    // in-flight slots than the queue can carry a post for. The bound holds per channel only: the receive tasks of every
    // channel and post_from_thread share the executor's one queue.
    static constexpr std::size_t k_slots = Slots;
    static_assert(k_slots <= freertos_executor::k_queue_depth, "the RX pool must not outsize the executor queue depth");

    lwip_rx_lifecycle(Owner &owner, Socket &socket, close_state &close, freertos_executor &ex)
            : m_owner(owner)
            , m_socket(socket)
            , m_close(close)
            , m_ex(ex)
            , m_exit(false)
            , m_pool(owner)
    {
    }

    lwip_rx_lifecycle(const lwip_rx_lifecycle &)            = delete;
    lwip_rx_lifecycle &operator=(const lwip_rx_lifecycle &) = delete;

    slot_type *acquire(TickType_t wait) noexcept
    {
        return m_pool.acquire(wait);
    }

    void release(slot_type &slot) noexcept
    {
        m_pool.release(slot);
    }

    std::size_t recv_into(slot_type &slot) noexcept
    {
        slot.len = m_socket.recv(slot.buffer);
        return slot.len;
    }

    bool socket_dropped() const noexcept
    {
        return m_socket.closed();
    }

    void started() noexcept
    {
        m_close.hold();
        m_ex.note_task_started();
    }

    void start_failed()
    {
        m_ex.note_task_exited();
        m_close.release();
    }

    bool live() const noexcept
    {
        return m_close.held();
    }

    void request_exit() noexcept
    {
        m_exit.store(true, std::memory_order_release);
    }

    bool exit_requested() const noexcept
    {
        return m_exit.load(std::memory_order_acquire);
    }

    void post_drop_observed() noexcept
    {
        const posted_work dropped{&invoke_drop_observed, this};
        while(!m_ex.post_from_task(dropped, portMAX_DELAY))
        {
        }
    }

    // The socket is closed by the task while it is live, or by the executor once the task has posted exited; on a drop
    // the task leaves the socket for the exited handler. After the exited post is accepted the task touches nothing the
    // channel owns, since the exited handler may lead to its destruction.
    void post_exited() noexcept
    {
        freertos_executor &ex = m_ex;
        const posted_work exited{&invoke_exited, this};
        while(!ex.post_from_task(exited, portMAX_DELAY))
        {
        }
    }

    void close_and_post_exited() noexcept
    {
        m_socket.close();
        post_exited();
    }

    static void invoke_feed(void *ctx) noexcept
    {
        auto &slot = *static_cast<slot_type *>(ctx);
        slot.owner->feed_from_slot(slot);
    }

    static void invoke_drop_observed(void *ctx) noexcept
    {
        static_cast<lwip_rx_lifecycle *>(ctx)->m_owner.rx_drop_observed();
    }

    static void invoke_exited(void *ctx) noexcept
    {
        auto &self = *static_cast<lwip_rx_lifecycle *>(ctx);
        self.m_socket.close();
        self.m_ex.note_task_exited();
        self.m_close.release();
    }

private:
    Owner &m_owner;
    Socket &m_socket;
    close_state &m_close;
    freertos_executor &m_ex;
    std::atomic<bool> m_exit;
    lwip_rx_slot_pool<Owner, Slots, Capacity> m_pool;
};

}

#endif
