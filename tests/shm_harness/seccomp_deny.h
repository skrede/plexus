#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SECCOMP_DENY_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SECCOMP_DENY_H

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>

#include <sys/prctl.h>
#include <sys/syscall.h>

#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>

namespace shm_harness {

#if defined(__x86_64__)
inline constexpr std::uint32_t k_audit_arch = AUDIT_ARCH_X86_64;
#elif defined(__aarch64__)
inline constexpr std::uint32_t k_audit_arch = AUDIT_ARCH_AARCH64;
#endif

// A classic-BPF filter per seccomp(2) answering the io_uring syscalls with EPERM, the errno the Docker default
// profile returns for them (moby PR 46762).
inline bool deny_io_uring() noexcept
{
#if defined(__x86_64__) || defined(__aarch64__)
    constexpr std::uint32_t deny = SECCOMP_RET_ERRNO | (EPERM & SECCOMP_RET_DATA);
    std::array<sock_filter, 9> program{{
            BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, arch)),
            BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, k_audit_arch, 1, 0),
            BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
            BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr)),
            BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_setup, 3, 0),
            BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_enter, 2, 0),
            BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_register, 1, 0),
            BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
            BPF_STMT(BPF_RET | BPF_K, deny),
    }};
    const sock_fprog filter{static_cast<unsigned short>(program.size()), program.data()};
    return ::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0 && ::prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &filter) == 0;
#else
    return false;
#endif
}

}

#endif
