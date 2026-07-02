#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_NOTIFIER_H

#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/windows/object_handle.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

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

// The cross-process wakeup -> reactor bridge for Windows, where there is no shared-page
// futex: a named auto-reset kernel event both peers derive from the region name is the
// doorbell, watched on the user's io_context via asio::windows::object_handle (IOCP). Each
// wake POSTS the drain onto the user's executor (Policy::post, never a serialized wrapper)
// and re-arms; the auto-reset event resets itself when a wait is satisfied. Non-copy/non-move:
// the asio object_handle holds a stable address and the in-flight handler captures `this`.
template<typename Policy>
class event_notifier
{
public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    event_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park, std::string_view region_name) noexcept
            : m_executor(executor)
            , m_word(word)
            , m_park(park)
            , m_event_name(make_event_name(region_name))
    {
    }

    // A single-process harness binds an owned park fallback and an anonymous (unnamed)
    // event, since no second process opens it by name.
    event_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word) noexcept
            : m_executor(executor)
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
    // drain/re-arm window would risk a silently lost wakeup. The event latches one wake.
    void signal() noexcept
    {
        m_word.fetch_add(1, std::memory_order_release);
        if(m_event != nullptr)
            ::SetEvent(static_cast<HANDLE>(m_event));
    }

    void arm(drain_fn drain)
    {
        m_drain_cb          = std::move(drain);
        const wchar_t *name = m_event_name.empty() ? nullptr : m_event_name.c_str();
        m_event             = ::CreateEventW(nullptr, FALSE, FALSE, name);
        if(m_event == nullptr)
            return; // bring-up failure: the seam stays inert (disarm is a no-op)
        m_doorbell.emplace(executor_io(m_executor));
        m_doorbell->assign(static_cast<HANDLE>(m_event));
        watch_doorbell();
    }

    // Mandated teardown order: clear the alive token, cancel the asio wait, detach the HANDLE
    // from asio (we own it), then close the event. The asio cancel does NOT un-post a drain a
    // prior on_signaled already handed to Policy::post; the alive token (checked by value in
    // that closure) makes the trailing post a no-op without dereferencing freed `this`.
    void disarm() noexcept
    {
        m_alive->store(false, std::memory_order_release);
        if(m_doorbell)
        {
            std::error_code ignore;
            m_doorbell->cancel(ignore);
            m_doorbell->release();
            m_doorbell.reset();
        }
        if(m_event != nullptr)
        {
            ::CloseHandle(static_cast<HANDLE>(m_event));
            m_event = nullptr;
        }
    }

private:
    // Both peers derive this identically from the region name (the 16-hex token), so the two
    // processes open the same auto-reset event in the per-session Local\ namespace.
    static std::wstring make_event_name(std::string_view region_name)
    {
        std::wstring name = L"Local\\plexus.n.";
        name.append(region_name.begin(), region_name.end());
        return name;
    }

    static ::asio::io_context &executor_io(typename Policy::executor_type ex) noexcept
    {
        return ex;
    }

    void watch_doorbell() noexcept
    {
        m_doorbell->async_wait([this](const std::error_code &ec) { on_signaled(ec); });
    }

    void on_signaled(const std::error_code &ec) noexcept
    {
        if(ec || !m_doorbell)
            return; // aborted on disarm, or already torn down: touch nothing
        Policy::post(m_executor, drain_post());
        watch_doorbell();
    }

    // The closure captures the alive token by value: a post that outlives a disarm (token went
    // false) no-ops instead of touching freed `this`. The token is a liveness flag, NOT shared
    // ownership (the entry stays the single owner and sequences teardown).
    drain_fn drain_post() noexcept
    {
        return [this, alive = m_alive]
        {
            if(alive->load(std::memory_order_acquire) && m_drain_cb)
                m_drain_cb();
        };
    }

    std::atomic<std::uint32_t> m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t> &m_word;
    std::atomic<std::uint32_t> &m_park;
    drain_fn m_drain_cb;

    std::wstring m_event_name;
    void *m_event = nullptr;
    std::optional<::asio::windows::object_handle> m_doorbell;
};

}

#endif
