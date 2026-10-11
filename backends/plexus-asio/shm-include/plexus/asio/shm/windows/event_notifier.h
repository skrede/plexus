#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H

#include "plexus/asio/shm/windows/event_doorbell.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <asio/windows/object_handle.hpp>
#include <asio/io_context.hpp>

#include <atomic>
#include <memory>
#include <string>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// Windows has no shared-page futex: every waiter owns an auto-reset event keyed by its region, cursor slot and the tag
// it stores in its cursor, watched on the user's io_context through IOCP. The waiter runs the per-waiter park cycle
// on the executor, and a producer sets only the events of the waiters it moved from PARKED to NOTIFIED.
template<typename Policy>
class event_notifier
{
    struct state
    {
        state(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header) noexcept
                : slot(0)
                , epoch(0)
                , doorbell()
                , header(header)
                , bridge(executor)
        {
        }

        std::uint32_t slot;
        std::uint32_t epoch;
        std::optional<::asio::windows::object_handle> doorbell;
        ::plexus::shm::control_header_t &header;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    event_notifier(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header, std::string_view region_name)
            : m_region(region_name)
            , m_state(std::make_shared<state>(executor, header))
            , m_bells()
    {
    }

    ~event_notifier()
    {
        disarm();
    }

    event_notifier(const event_notifier &)            = delete;
    event_notifier &operator=(const event_notifier &) = delete;
    event_notifier(event_notifier &&)                 = delete;
    event_notifier &operator=(event_notifier &&)      = delete;

    void signal() noexcept
    {
        const std::uint32_t mask = ::plexus::shm::signal_parked(m_state->header);
        if(mask != 0)
            m_bells.ring(m_state->header, m_region, mask);
    }

    // The tag is published before the first park, so a producer that sees the slot PARKED names this event.
    ::plexus::shm::arm_status arm(std::uint32_t slot, drain_fn drain)
    {
        state &s = *m_state;
        s.slot   = slot;
        s.bridge.arm(std::move(drain));
        const std::optional<detail::own_event> event = detail::create_own_event(m_region, slot);
        if(!event)
            return ::plexus::shm::arm_status::exhausted;
        s.header.cursors[slot].waiter_tag.store(event->tag, std::memory_order_release);
        ::plexus::shm::park_clear(s.header, slot);
        s.doorbell.emplace(s.bridge.executor());
        s.doorbell->assign(event->handle);
        cycle(m_state, ++s.epoch);
        return ::plexus::shm::arm_status::armed;
    }

    // assign() handed the event to the object_handle, whose close() is the one CloseHandle. The epoch bump turns away
    // every cycle and completion still queued for this arm.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(!s.doorbell)
            return;
        ++s.epoch;
        std::error_code ignore;
        s.doorbell->cancel(ignore);
        s.doorbell->close(ignore);
        s.doorbell.reset();
        ::plexus::shm::park_clear(s.header, s.slot);
        s.header.cursors[s.slot].waiter_tag.store(0, std::memory_order_release);
    }

private:
    std::string m_region;
    std::shared_ptr<state> m_state;
    detail::event_doorbell_cache m_bells;

    // The snapshot precedes the posted drain, so it covers every publish up to the snapshot; a later publish either
    // fails the recheck, which re-enters the cycle through the executor, or finds the slot PARKED and sets the event.
    static void cycle(const std::shared_ptr<state> &s, std::uint32_t epoch)
    {
        if(!s->doorbell || s->epoch != epoch)
            return;
        const std::uint32_t snapshot = ::plexus::shm::park_snapshot(s->header);
        ::plexus::shm::detail::post_drain(s, s->bridge);
        ::plexus::shm::park_store(s->header, s->slot);
        if(::plexus::shm::park_recheck(s->header, snapshot))
        {
            s->doorbell->async_wait([s, epoch](const std::error_code &ec) { on_signaled(s, epoch, ec); });
            return;
        }
        ::plexus::shm::park_clear(s->header, s->slot);
        Policy::post(s->bridge.executor(), [s, epoch] { cycle(s, epoch); });
    }

    static void on_signaled(const std::shared_ptr<state> &s, std::uint32_t epoch, const std::error_code &ec)
    {
        if(ec || !s->doorbell || s->epoch != epoch)
            return;
        ::plexus::shm::park_clear(s->header, s->slot);
        cycle(s, epoch);
    }
};

}

#endif
