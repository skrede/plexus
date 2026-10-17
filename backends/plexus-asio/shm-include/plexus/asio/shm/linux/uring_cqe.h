#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_CQE_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_CQE_H

#include <liburing.h>

#include <cerrno>
#include <cstdint>
#include <algorithm>

namespace plexus::asio::shm {

// Ordered by strength: a batch of completions acts on its strongest verdict.
enum class cqe_verdict : std::uint8_t
{
    rearm,
    stop,
    demote
};

// A wake, a value mismatch at registration (-EAGAIN) and an interrupted wait re-arm; a canceled wait stops; any other
// error means the kernel cannot serve the futex wait, so re-submitting would only spin.
inline cqe_verdict classify_futex_cqe(std::int32_t res) noexcept
{
    if(res >= 0 || res == -EAGAIN || res == -EINTR)
        return cqe_verdict::rearm;
    return res == -ECANCELED ? cqe_verdict::stop : cqe_verdict::demote;
}

inline cqe_verdict reap_futex_cqes(io_uring &ring) noexcept
{
    cqe_verdict verdict = cqe_verdict::rearm;
    io_uring_cqe *cqe   = nullptr;
    while(::io_uring_peek_cqe(&ring, &cqe) == 0 && cqe != nullptr)
    {
        verdict = std::max(verdict, classify_futex_cqe(cqe->res));
        ::io_uring_cqe_seen(&ring, cqe);
    }
    return verdict;
}

}

#endif
