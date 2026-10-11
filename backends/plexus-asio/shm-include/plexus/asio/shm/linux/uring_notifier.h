#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H

#include "plexus/asio/shm/linux/uring_cqe.h"
#include "plexus/asio/shm/linux/uring_setup.h"

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <liburing.h>

#include <linux/futex.h>

#include <atomic>
#include <memory>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// Lands a cross-process futex wake in the user's reactor turn without a plexus-owned thread: a
// private io_uring submits IORING_OP_FUTEX_WAIT on its slot's park word, and its registered eventfd
// is the doorbell watched on the user's io_context. A completion the kernel cannot serve posts the
// injected demotion as unsupported, and a wait it cannot queue again as exhausted, instead of re-submitting.
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
                , demote_cb()
                , bridge(executor)
        {
        }

        io_uring ring;
        std::uint32_t slot;
        bool ring_live;
        detail::fd_doorbell doorbell;
        ::plexus::shm::control_header_t &header;
        plexus::detail::move_only_function<void(::plexus::shm::arm_status)> demote_cb;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn  = plexus::detail::move_only_function<void()>;
    using demote_fn = plexus::detail::move_only_function<void(::plexus::shm::arm_status)>;

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

    ::plexus::shm::arm_status arm(std::uint32_t slot, drain_fn drain, demote_fn demote)
    {
        state &s = *m_state;
        s.slot   = slot;
        s.bridge.arm(std::move(drain));
        s.demote_cb = std::move(demote);
        if(const ::plexus::shm::arm_status status = detail::uring_bring_up(s.ring, s.doorbell); status != ::plexus::shm::arm_status::armed)
            return status;
        s.ring_live = true;
        s.doorbell.attach(s.bridge.executor());
        if(!park_and_wait(s, ::plexus::shm::park_snapshot(s.header)))
            return ::plexus::shm::arm_status::exhausted;
        watch(m_state);
        return ::plexus::shm::arm_status::armed;
    }

    // Exiting the io_uring drops the in-flight futex wait; the eventfd it signals closes after it.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        s.demote_cb = nullptr;
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
    // A publish since the snapshot clears the word, so the wait completes at registration (the kernel compares the
    // word against PARKED) and the handler runs another drain instead of blocking. FUTEX_BITSET_MATCH_ANY plus
    // FUTEX2_SIZE_U32 match the SHARED 32-bit word the producer wakes. A wait that cannot be queued leaves no PARKED
    // word behind.
    static bool park_and_wait(state &s, std::uint32_t snapshot) noexcept
    {
        io_uring_sqe *sqe = ::io_uring_get_sqe(&s.ring);
        if(sqe == nullptr)
            return false;
        std::atomic<std::uint32_t> &park = ::plexus::shm::park_word(s.header, s.slot);
        ::plexus::shm::park_store(s.header, s.slot);
        if(!::plexus::shm::park_recheck(s.header, snapshot))
            ::plexus::shm::park_clear(s.header, s.slot);
        ::io_uring_prep_futex_wait(sqe, reinterpret_cast<std::uint32_t *>(&park), ::plexus::shm::k_park_parked, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
        if(::io_uring_submit(&s.ring) > 0)
            return true;
        ::plexus::shm::park_clear(s.header, s.slot);
        return false;
    }

    static void watch(const std::shared_ptr<state> &s)
    {
        s->doorbell.watch([s](const std::error_code &ec) { on_doorbell(s, ec); });
    }

    // The generation counter coalesces a flood, so one drain after reaping every CQE suffices. The snapshot precedes
    // the posted drain, so the drain covers every publish up to it even when another thread runs the executor.
    static void on_doorbell(const std::shared_ptr<state> &s, const std::error_code &ec)
    {
        if(ec || !s->doorbell.attached())
            return;
        s->doorbell.drain();
        const cqe_verdict verdict = reap_futex_cqes(s->ring);
        ::plexus::shm::park_clear(s->header, s->slot);
        if(verdict == cqe_verdict::stop)
            return;
        if(verdict == cqe_verdict::demote)
            return post_demotion(s, ::plexus::shm::arm_status::unsupported);
        const std::uint32_t snapshot = ::plexus::shm::park_snapshot(s->header);
        ::plexus::shm::detail::post_drain(s, s->bridge);
        if(!park_and_wait(*s, snapshot))
            return post_demotion(s, ::plexus::shm::arm_status::exhausted);
        watch(s);
    }

    // The demotion disarms this leg, which clears the callback, so it is moved out before it runs.
    static void post_demotion(const std::shared_ptr<state> &s, ::plexus::shm::arm_status reason)
    {
        Policy::post(s->bridge.executor(),
                     [s, reason]
                     {
                         demote_fn demotion = std::move(s->demote_cb);
                         s->demote_cb       = nullptr;
                         if(demotion)
                             demotion(reason);
                     });
    }

    std::shared_ptr<state> m_state;
};

}

#endif
