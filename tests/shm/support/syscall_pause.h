#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SYSCALL_PAUSE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SYSCALL_PAUSE_H

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <bit>
#include <cerrno>
#include <future>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

#include <poll.h>
#include <unistd.h>

#include <sys/ioctl.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>

namespace plexus::testing {

#if defined(__x86_64__)
inline constexpr std::uint32_t k_audit_arch = AUDIT_ARCH_X86_64;
#elif defined(__aarch64__)
inline constexpr std::uint32_t k_audit_arch = AUDIT_ARCH_AARCH64;
#elif defined(__riscv) && __riscv_xlen == 64
inline constexpr std::uint32_t k_audit_arch = AUDIT_ARCH_RISCV64;
#else
    #error "syscall_pause names no audit architecture for this target"
#endif

// A condition on the low 32 bits of one syscall argument; an equal condition also requires the high 32 bits to be 0.
struct syscall_arg
{
    std::uint32_t index;
    std::uint32_t value;
    bool equal;
};

// A classic BPF program that allows every call but the matching one, which it hands to the listener.
class syscall_filter
{
public:
    explicit syscall_filter(std::int32_t nr)
            : m_code()
            , m_to_allow()
    {
        load(offsetof(seccomp_data, arch));
        allow_unless(k_audit_arch);
        load(offsetof(seccomp_data, nr));
        allow_unless(static_cast<std::uint32_t>(nr));
    }

    void require(const syscall_arg &arg)
    {
        load(arg_word(arg.index, false));
        if(!arg.equal)
        {
            allow_if(arg.value);
            return;
        }
        allow_unless(arg.value);
        load(arg_word(arg.index, true));
        allow_unless(0);
    }

    std::vector<sock_filter> finish()
    {
        m_code.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_USER_NOTIF));
        const std::size_t allow = m_code.size();
        m_code.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
        for(const auto &[at, on_equal] : m_to_allow)
            (on_equal ? m_code[at].jt : m_code[at].jf) = static_cast<std::uint8_t>(allow - at - 1);
        return std::move(m_code);
    }

private:
    std::vector<sock_filter> m_code;
    std::vector<std::pair<std::size_t, bool>> m_to_allow;

    static std::uint32_t arg_word(std::uint32_t index, bool high)
    {
        const bool upper_half = high == (std::endian::native == std::endian::little);
        return static_cast<std::uint32_t>(offsetof(seccomp_data, args) + index * sizeof(std::uint64_t) + (upper_half ? 4 : 0));
    }

    void load(std::uint32_t offset)
    {
        m_code.push_back(BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offset));
    }

    void allow_if(std::uint32_t k)
    {
        m_to_allow.emplace_back(m_code.size(), true);
        m_code.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, k, 0, 0));
    }

    void allow_unless(std::uint32_t k)
    {
        m_to_allow.emplace_back(m_code.size(), false);
        m_code.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, k, 0, 0));
    }
};

// Runs work on a thread of its own whose matching syscall waits in a seccomp user notification (seccomp_unotify(2))
// until the test answers it. The filter goes in without TSYNC, so it binds the worker thread alone.
class syscall_pause
{
public:
    syscall_pause(std::int32_t nr, std::vector<syscall_arg> args, plexus::detail::move_only_function<void()> work)
            : m_caught()
            , m_ready()
            , m_worker([this, nr, args = std::move(args), work = std::move(work)]() mutable { run(nr, args, work); })
            , m_listener(m_ready.get_future().get())
    {
    }

    ~syscall_pause()
    {
        if(m_listener >= 0)
            ::close(m_listener);
        m_worker.join();
    }

    syscall_pause(const syscall_pause &)            = delete;
    syscall_pause &operator=(const syscall_pause &) = delete;

    seccomp_notif wait()
    {
        INFO("seccomp install errno " << -m_listener);
        REQUIRE(m_listener >= 0);
        pollfd ready{m_listener, POLLIN, 0};
        REQUIRE(::poll(&ready, 1, 10'000) == 1);
        m_caught = seccomp_notif{};
        REQUIRE(::ioctl(m_listener, SECCOMP_IOCTL_NOTIF_RECV, &m_caught) == 0);
        return m_caught;
    }

    void fail(std::int32_t errno_value)
    {
        seccomp_notif_resp answer{};
        answer.id    = m_caught.id;
        answer.error = -errno_value;
        REQUIRE(::ioctl(m_listener, SECCOMP_IOCTL_NOTIF_SEND, &answer) == 0);
    }

private:
    seccomp_notif m_caught;
    std::promise<std::int32_t> m_ready;
    std::thread m_worker;
    std::int32_t m_listener;

    void run(std::int32_t nr, const std::vector<syscall_arg> &args, plexus::detail::move_only_function<void()> &work)
    {
        const std::int32_t listener = install(nr, args);
        m_ready.set_value(listener);
        if(listener >= 0)
            work();
    }

    static std::int32_t install(std::int32_t nr, const std::vector<syscall_arg> &args)
    {
        syscall_filter filter{nr};
        for(const syscall_arg &arg : args)
            filter.require(arg);
        std::vector<sock_filter> code = filter.finish();
        const sock_fprog program{static_cast<unsigned short>(code.size()), code.data()};
        if(::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
            return -errno;
        const long listener = ::syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_NEW_LISTENER, &program);
        return listener < 0 ? -errno : static_cast<std::int32_t>(listener);
    }
};

}

#endif
