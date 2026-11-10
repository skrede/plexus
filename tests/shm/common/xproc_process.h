#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_PROCESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_PROCESS_H

#include "xproc_launch.h"

#if !defined(_WIN32)
    #include <cerrno>
    #include <csignal>

    #include <sys/wait.h>
#endif

#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::testing {

namespace detail {

#if defined(_WIN32)

// 128 + SIGKILL, the status a POSIX child killed by SIGKILL reports.
inline constexpr UINT k_xproc_killed_status = 137;

inline std::int32_t xproc_collect(xproc_child_handle child)
{
    DWORD code = 1;
    ::GetExitCodeProcess(child, &code);
    ::CloseHandle(child);
    return static_cast<std::int32_t>(code);
}

inline std::optional<std::int32_t> xproc_reap_within(xproc_child_handle child, std::chrono::milliseconds bound)
{
    const auto ms = std::clamp<std::int64_t>(bound.count(), 0, static_cast<std::int64_t>(INFINITE) - 1);
    if(::WaitForSingleObject(child, static_cast<DWORD>(ms)) != WAIT_OBJECT_0)
        return std::nullopt;
    return xproc_collect(child);
}

inline std::int32_t xproc_reap(xproc_child_handle child)
{
    ::WaitForSingleObject(child, INFINITE);
    return xproc_collect(child);
}

inline void xproc_terminate(xproc_child_handle child)
{
    ::TerminateProcess(child, k_xproc_killed_status);
}

#else

inline std::int32_t xproc_decode(int status)
{
    if(WIFEXITED(status))
        return WEXITSTATUS(status);
    if(WIFSIGNALED(status))
        return 128 + WTERMSIG(status);
    return -1;
}

// A child that can no longer be waited on (ECHILD) reports -1 rather than staying running forever.
inline std::optional<std::int32_t> xproc_try_reap(xproc_child_handle child)
{
    int status = 0;
    pid_t rc   = 0;
    while((rc = ::waitpid(child, &status, WNOHANG)) < 0 && errno == EINTR)
        ;
    if(rc == 0)
        return std::nullopt;
    return rc == child ? xproc_decode(status) : -1;
}

inline std::optional<std::int32_t> xproc_reap_within(xproc_child_handle child, std::chrono::milliseconds bound)
{
    const auto deadline                = std::chrono::steady_clock::now() + bound;
    std::optional<std::int32_t> status = xproc_try_reap(child);
    while(!status && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        status = xproc_try_reap(child);
    }
    return status;
}

inline std::int32_t xproc_reap(xproc_child_handle child)
{
    int status = 0;
    pid_t rc   = 0;
    while((rc = ::waitpid(child, &status, 0)) < 0 && errno == EINTR)
        ;
    return rc == child ? xproc_decode(status) : -1;
}

inline void xproc_terminate(xproc_child_handle child)
{
    ::kill(child, SIGKILL);
}

#endif

}

class xproc_process
{
public:
    static std::optional<xproc_process> spawn(std::string_view key, const std::vector<std::string> &args)
    {
        const std::optional<detail::xproc_child_handle> child = detail::xproc_spawn(key, args);
        if(!child)
            return std::nullopt;
        return xproc_process(*child);
    }

    xproc_process(xproc_process &&other) noexcept
            : m_status(other.m_status)
            , m_child(std::exchange(other.m_child, std::nullopt))
    {
    }

    xproc_process &operator=(xproc_process &&other) noexcept
    {
        if(this != &other)
        {
            kill();
            m_status = other.m_status;
            m_child  = std::exchange(other.m_child, std::nullopt);
        }
        return *this;
    }

    xproc_process(const xproc_process &)            = delete;
    xproc_process &operator=(const xproc_process &) = delete;

    ~xproc_process()
    {
        kill();
    }

    std::optional<std::int32_t> wait_for(std::chrono::milliseconds bound)
    {
        if(m_child && !m_status)
            m_status = detail::xproc_reap_within(*m_child, bound);
        return m_status;
    }

    std::int32_t wait()
    {
        if(m_child && !m_status)
            m_status = detail::xproc_reap(*m_child);
        return m_status.value_or(-1);
    }

    void kill()
    {
        if(!running())
            return;
        detail::xproc_terminate(*m_child);
        wait();
    }

    // Reaping only records an exit that already happened, so the const query may observe it.
    bool running() const
    {
        if(m_child && !m_status)
            m_status = detail::xproc_reap_within(*m_child, std::chrono::milliseconds(0));
        return m_child && !m_status;
    }

private:
    explicit xproc_process(detail::xproc_child_handle child)
            : m_status()
            , m_child(child)
    {
    }

    mutable std::optional<std::int32_t> m_status;
    std::optional<detail::xproc_child_handle> m_child;
};

}

#endif
