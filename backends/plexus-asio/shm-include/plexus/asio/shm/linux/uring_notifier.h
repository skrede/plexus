#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <liburing.h>

#include <linux/futex.h>
#include <sys/eventfd.h>

#include <atomic>
#include <cerrno>
#include <memory>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// Lands a cross-process futex wake in the user's reactor turn without a plexus-owned thread: a
// private io_uring submits IORING_OP_FUTEX_WAIT on its slot's park word, and its registered eventfd
// is the doorbell watched on the user's io_context.
template<typename Policy>
class ring_notifier_uring
{
    struct state
    {
        state(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header) noexcept
                : ring()
                , slot(0)
                , ring_live(false)
                , doorbell()
                , header(header)
                , bridge(executor)
        {
        }

        io_uring ring;
        std::uint32_t slot;
        bool ring_live;
        detail::fd_doorbell doorbell;
        ::plexus::shm::control_header_t &header;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier_uring(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header, std::string_view)
            : m_state(std::make_shared<state>(executor, header))
    {
    }

    ~ring_notifier_uring()
    {
        disarm();
    }

    ring_notifier_uring(const ring_notifier_uring &)            = delete;
    ring_notifier_uring &operator=(const ring_notifier_uring &) = delete;
    ring_notifier_uring(ring_notifier_uring &&)                 = delete;
    ring_notifier_uring &operator=(ring_notifier_uring &&)      = delete;

    void signal() noexcept
    {
        ::plexus::native::notifier_signal(m_state->header);
    }

    ::plexus::shm::arm_status arm(std::uint32_t slot, drain_fn drain)
    {
        state &s = *m_state;
        s.slot   = slot;
        s.bridge.arm(std::move(drain));
        if(const int rc = ::io_uring_queue_init(k_ring_depth, &s.ring, 0); rc != 0)
            return rc == -EPERM || rc == -ENOSYS ? ::plexus::shm::arm_status::denied : ::plexus::shm::arm_status::exhausted;
        s.ring_live = true;
        if(!s.doorbell.adopt(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK), detail::fd_doorbell::kind::counter) || ::io_uring_register_eventfd(&s.ring, s.doorbell.native_handle()) != 0)
            return ::plexus::shm::arm_status::exhausted;
        s.doorbell.attach(s.bridge.executor());
        park_and_wait(s, ::plexus::shm::park_snapshot(s.header));
        watch(m_state);
        return ::plexus::shm::arm_status::armed;
    }

    // Exiting the io_uring drops the in-flight futex wait; the eventfd it signals closes after it.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        s.doorbell.detach();
        if(s.ring_live)
        {
            ::io_uring_queue_exit(&s.ring);
            ::plexus::shm::park_clear(s.header, s.slot);
            s.ring_live = false;
        }
        s.doorbell.close();
    }

private:
    static constexpr unsigned k_ring_depth = 8;

    // A publish since the snapshot clears the word, so the wait completes at registration (the kernel compares the
    // word against PARKED) and the handler runs another drain instead of blocking. FUTEX_BITSET_MATCH_ANY plus
    // FUTEX2_SIZE_U32 match the SHARED 32-bit word the producer wakes.
    static void park_and_wait(state &s, std::uint32_t snapshot) noexcept
    {
        io_uring_sqe *sqe = ::io_uring_get_sqe(&s.ring);
        if(sqe == nullptr)
            return;
        std::atomic<std::uint32_t> &park = ::plexus::shm::park_word(s.header, s.slot);
        ::plexus::shm::park_store(s.header, s.slot);
        if(!::plexus::shm::park_recheck(s.header, snapshot))
            ::plexus::shm::park_clear(s.header, s.slot);
        ::io_uring_prep_futex_wait(sqe, reinterpret_cast<std::uint32_t *>(&park), ::plexus::shm::k_park_parked, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
        ::io_uring_submit(&s.ring);
    }

    static void watch(const std::shared_ptr<state> &s)
    {
        s->doorbell.watch([s](const std::error_code &ec) { on_doorbell(s, ec); });
    }

    // The snapshot precedes the posted drain, so the drain covers every publish up to it even when another thread
    // runs the executor.
    static void on_doorbell(const std::shared_ptr<state> &s, const std::error_code &ec)
    {
        if(ec || !s->doorbell.attached())
            return;
        s->doorbell.drain();
        reap_cqe(*s);
        ::plexus::shm::park_clear(s->header, s->slot);
        const std::uint32_t snapshot = ::plexus::shm::park_snapshot(s->header);
        ::plexus::shm::detail::post_drain(s, s->bridge);
        park_and_wait(*s, snapshot);
        watch(s);
    }

    // The generation counter coalesces a flood, so one drain after reaping every CQE suffices.
    static void reap_cqe(state &s) noexcept
    {
        io_uring_cqe *cqe = nullptr;
        while(::io_uring_peek_cqe(&s.ring, &cqe) == 0 && cqe != nullptr)
            ::io_uring_cqe_seen(&s.ring, cqe);
    }

    std::shared_ptr<state> m_state;
};

}

#endif
