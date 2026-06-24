#ifndef HPP_GUARD_PLEXUS_ASIO_SPIN_RUN_LOOP_H
#define HPP_GUARD_PLEXUS_ASIO_SPIN_RUN_LOOP_H

#include "plexus/detail/cpu_relax.h"

#include <asio/io_context.hpp>

#include <cstdint>

namespace plexus::asio {

// Opt-in spin-then-park driver over a user-owned io_context: poll_spin() drains ready work,
// then on idle spins up to spin_budget polls (relaxing the core) so a datagram landing within
// the window is caught hot, skipping the park->wake context switch the blocking reactor pays on
// every arrival. spin_budget == 0 makes run() semantically io.run() (byte-identical to the
// default reactor). It is a DRIVE strategy, not an executor: posting still goes through the
// io_context. Borrows the io_context by reference.
class spin_run_loop
{
public:
    // The conservative default, set from a {0,16,64,256,1k,4k} x rate sweep on the bench rig:
    // each idle poll is an epoll_wait(0) probe (~sub-us), so the window trades a bounded
    // idle-syscall burn for catching a back-to-back arrival hot while still parking when idle.
    static constexpr std::uint32_t default_spin_budget = 256;

    explicit spin_run_loop(::asio::io_context &io, std::uint32_t spin_budget = default_spin_budget) noexcept
            : m_io(io)
            , m_spin_budget(spin_budget)
    {
    }

    spin_run_loop(const spin_run_loop &)            = delete;
    spin_run_loop &operator=(const spin_run_loop &) = delete;
    spin_run_loop(spin_run_loop &&)                 = delete;
    spin_run_loop &operator=(spin_run_loop &&)      = delete;

    bool poll_spin()
    {
        if(m_io.poll() != 0)
            return true;
        for(std::uint32_t spun = 0; spun < m_spin_budget; ++spun)
        {
            plexus::detail::cpu_relax();
            if(m_io.poll() != 0)
                return true;
        }
        return false;
    }

    void run()
    {
        while(!m_io.stopped())
        {
            if(!poll_spin())
                m_io.run_one();
        }
    }

    std::uint32_t spin_budget() const noexcept
    {
        return m_spin_budget;
    }

private:
    ::asio::io_context &m_io;
    std::uint32_t m_spin_budget;
};

}

#endif
