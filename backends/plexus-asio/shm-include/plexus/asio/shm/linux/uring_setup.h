#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_SETUP_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_SETUP_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/shm/arm_status.h"

#include <liburing.h>

#include <sys/eventfd.h>

#include <cerrno>

namespace plexus::asio::shm::detail {

inline constexpr unsigned k_uring_depth = 8;

// IORING_OP_FUTEX_WAIT exists from kernel 6.7 (io_uring_prep_futex_wait(3)); an older kernel accepts the ring but
// lacks the opcode.
inline bool uring_supports_futex_wait(io_uring &ring) noexcept
{
    io_uring_probe *probe = ::io_uring_get_probe_ring(&ring);
    if(probe == nullptr)
        return false;
    const bool supported = ::io_uring_opcode_supported(probe, IORING_OP_FUTEX_WAIT) != 0;
    ::io_uring_free_probe(probe);
    return supported;
}

// A container seccomp profile refuses io_uring_setup with EPERM (Docker, moby PR 46762) or ENOSYS (podman's
// default); the kernel's io_uring_disabled sysctl answers EPERM as well.
inline ::plexus::shm::arm_status uring_setup_refusal(int rc) noexcept
{
    return rc == -EPERM || rc == -ENOSYS ? ::plexus::shm::arm_status::denied : ::plexus::shm::arm_status::exhausted;
}

// Leaves the ring live and the doorbell registered only when it reports armed.
inline ::plexus::shm::arm_status uring_bring_up(io_uring &ring, fd_doorbell &doorbell) noexcept
{
    if(const int rc = ::io_uring_queue_init(k_uring_depth, &ring, 0); rc != 0)
        return uring_setup_refusal(rc);
    if(!uring_supports_futex_wait(ring))
    {
        ::io_uring_queue_exit(&ring);
        return ::plexus::shm::arm_status::unsupported;
    }
    if(!doorbell.adopt(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK), fd_doorbell::kind::counter) || ::io_uring_register_eventfd(&ring, doorbell.native_handle()) != 0)
    {
        ::io_uring_queue_exit(&ring);
        doorbell.close();
        return ::plexus::shm::arm_status::exhausted;
    }
    return ::plexus::shm::arm_status::armed;
}

}

#endif
