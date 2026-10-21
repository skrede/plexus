#ifndef HPP_GUARD_PLEXUS_TESTS_ASIO_QUEUED_TEARDOWN_H
#define HPP_GUARD_PLEXUS_TESTS_ASIO_QUEUED_TEARDOWN_H

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <chrono>
#include <thread>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::testing {

inline constexpr std::int32_t k_teardown_iterations = 8;

// Destroys its victim from a timer armed to expire strictly before the victim's own completion is due,
// so the destruction runs in the reactor pass that already queued that completion: the reactor hands
// over expired timers after the ready descriptors and in expiry order, and cancel() cannot recall a
// completion it has queued.
template<typename Victim>
class queued_teardown
{
public:
    queued_teardown(::asio::io_context &io, Victim &victim)
            : m_io(io)
            , m_timer(io, std::chrono::milliseconds(1))
    {
        m_timer.async_wait([&victim](std::error_code) { victim.reset(); });
    }

    void run_after(std::chrono::milliseconds past_both)
    {
        std::this_thread::sleep_for(past_both);
        m_io.poll();
        m_io.run_for(std::chrono::milliseconds(50));
    }

private:
    ::asio::io_context &m_io;
    ::asio::steady_timer m_timer;
};

// queued_teardown's timing with a caller-supplied action in place of the destruction: the action runs in the reactor
// pass that already queued the completion it races.
template<typename Action>
class queued_action
{
public:
    queued_action(::asio::io_context &io, Action action)
            : m_io(io)
            , m_timer(io, std::chrono::milliseconds(1))
    {
        m_timer.async_wait([action = std::move(action)](std::error_code) mutable { action(); });
    }

    void run_after(std::chrono::milliseconds past_both)
    {
        std::this_thread::sleep_for(past_both);
        m_io.poll();
        m_io.run_for(std::chrono::milliseconds(50));
    }

private:
    ::asio::io_context &m_io;
    ::asio::steady_timer m_timer;
};

}

#endif
