#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <asio/windows/object_handle.hpp>
#include <asio/io_context.hpp>

#include <windows.h>

#include <atomic>
#include <memory>
#include <string>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// Windows has no shared-page futex: a named auto-reset kernel event both peers derive from the
// region name is the doorbell, watched on the user's io_context through IOCP. The event resets
// itself when a wait is satisfied.
template<typename Policy>
class event_notifier
{
    struct state
    {
        explicit state(typename Policy::executor_type executor) noexcept
                : doorbell()
                , bridge(executor)
        {
        }

        std::optional<::asio::windows::object_handle> doorbell;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    event_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park, std::string_view region_name)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_event(nullptr)
            , m_event_name(make_event_name(region_name))
            , m_state(std::make_shared<state>(executor))
            , m_word(word)
            , m_park(park)
    {
    }

    // A single-process harness binds an owned park fallback and an unnamed event no second
    // process opens.
    event_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_event(nullptr)
            , m_event_name()
            , m_state(std::make_shared<state>(executor))
            , m_word(word)
            , m_park(m_park_fallback)
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

    // Ungated: an auto-reset event has no value-compare, so a park-gated skip during the
    // drain/re-arm window could lose a wake. The event latches one.
    void signal() noexcept
    {
        m_word.fetch_add(1, std::memory_order_release);
        if(m_event != nullptr)
            ::SetEvent(static_cast<HANDLE>(m_event));
    }

    void arm(drain_fn drain)
    {
        state &s = *m_state;
        s.bridge.arm(std::move(drain));
        const wchar_t *name = m_event_name.empty() ? nullptr : m_event_name.c_str();
        m_event             = ::CreateEventW(nullptr, FALSE, FALSE, name);
        if(m_event == nullptr)
            return;
        s.doorbell.emplace(s.bridge.executor());
        s.doorbell->assign(static_cast<HANDLE>(m_event));
        watch(m_state);
    }

    // assign() handed the event to the object_handle, which has no release(): its close() is the
    // one CloseHandle, so the raw handle is only forgotten here.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(s.doorbell)
        {
            std::error_code ignore;
            s.doorbell->cancel(ignore);
            s.doorbell->close(ignore);
            s.doorbell.reset();
        }
        m_event = nullptr;
    }

private:
    // Both peers derive the name identically, so the two processes open the same event in the
    // per-session Local\ namespace.
    static std::wstring make_event_name(std::string_view region_name)
    {
        std::wstring name = L"Local\\plexus.n.";
        name.append(region_name.begin(), region_name.end());
        return name;
    }

    static void watch(const std::shared_ptr<state> &s)
    {
        s->doorbell->async_wait([s](const std::error_code &ec) { on_signaled(s, ec); });
    }

    static void on_signaled(const std::shared_ptr<state> &s, const std::error_code &ec)
    {
        if(ec || !s->doorbell)
            return;
        ::plexus::shm::detail::post_drain(s, s->bridge);
        watch(s);
    }

    std::atomic<std::uint32_t> m_park_fallback;
    void *m_event;
    std::wstring m_event_name;
    std::shared_ptr<state> m_state;
    std::atomic<std::uint32_t> &m_word;
    std::atomic<std::uint32_t> &m_park;
};

}

#endif
