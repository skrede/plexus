#ifndef HPP_GUARD_PLEXUS_ASIO_SPIN_RUN_LOOP_H
#define HPP_GUARD_PLEXUS_ASIO_SPIN_RUN_LOOP_H

#include "plexus/detail/cpu_relax.h"

#include <asio/io_context.hpp>

#include <cstdint>

namespace plexus::asio {

// Opt-in spin-then-park driver over a user-owned io_context: the socket analog of
// the SHM slot_subscriber spin and the inproc step() executor. poll_spin() drains
// ready work; on idle it spins up to spin_budget polls relaxing the core so a
// datagram landing within the window is caught while the receiver is still hot --
// skipping the park->wake context switch the blocking reactor pays on every arrival
// (the loopback receive-path cost the default park-reactor cannot avoid). run()
// drives poll_spin() then parks on the reactor (epoll) at ~0% CPU between bursts.
//
// spin_budget is a required-WITH-default consumer policy knob: 0 = drain-then-park
// only (run() is then semantically io.run(), byte-identical to the default reactor);
// large = effectively always spin. The knee is rate-dependent -- the spin window
// only catches the next datagram when arrival falls inside it -- so no fixed budget
// is optimal everywhere; the default is the conservative swept knee (see
// default_spin_budget). It is a DRIVE strategy, not an executor: posting still goes
// through the io_context (asio_policy::post). Upholds user-owns-the-executor -- it
// spins the USER's loop, no background thread.
//
// Borrows the io_context BY REFERENCE; non-copy/non-move owning driver.
class spin_run_loop
{
public:
    // The conservative default spin budget, set from a {0,16,64,256,1k,4k} x rate
    // sweep on the bench rig (asio-spin-budget-sweep): each idle poll is an
    // epoll_wait(0) probe (~sub-us), so the window trades a bounded idle-syscall
    // burn for catching a back-to-back arrival hot; the default reclaims the
    // park->wake cost on the high-rate path while still parking when genuinely idle,
    // with the knob for a latency-maximalist consumer to opt up.
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

    // Non-blocking drive step: drain ready handlers; on idle spin up to spin_budget
    // polls (each an epoll_wait(0) probe + core relax) catching a fresh arrival
    // without the receiver ever sleeping. Returns true if any handler ran. poll()
    // drains the whole recv-completion -> posted on_data chain in one call, so a
    // caught datagram is delivered within the same step. The hot-loop / cooperative
    // building block (a deadline loop drives this; run() builds the park on it).
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

    // Steady-state app driver: spin, and when the window expires idle park on the
    // reactor for one event at ~0% CPU. Exits when the io_context is stopped
    // (io.stop() from a signal handler or another thread breaks the park).
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
    std::uint32_t       m_spin_budget;
};

}

#endif
