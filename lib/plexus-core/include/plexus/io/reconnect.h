#ifndef HPP_GUARD_PLEXUS_IO_RECONNECT_H
#define HPP_GUARD_PLEXUS_IO_RECONNECT_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/reconnect_config.h"

#include <chrono>
#include <random>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::io {

// Full-jitter exponential backoff: delay = uniform(0, min(max_delay, min_delay * 2^attempt)).
// The shift is capped at 20 so the 64-bit scale cannot overflow.
template<typename Rng>
std::chrono::milliseconds compute_backoff(const reconnect_config &cfg, std::uint32_t attempt, Rng &rng)
{
    const auto shift = std::min(attempt, std::uint32_t{20});
    auto ceiling     = cfg.min_delay * (std::uint64_t{1} << shift);
    if(ceiling > cfg.max_delay)
        ceiling = cfg.max_delay;
    std::uniform_int_distribution<std::int64_t> dist{0, static_cast<std::int64_t>(ceiling.count())};
    return std::chrono::milliseconds{dist(rng)};
}

template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy>
class reconnect
{
public:
    using timer_type = typename Policy::timer_type;

    reconnect(Transport &transport, typename Policy::executor_type executor, const reconnect_config &cfg, io::endpoint endpoint, std::uint64_t seed = 0)
            : m_transport(transport)
            , m_cfg(cfg)
            , m_endpoint(std::move(endpoint))
            , m_backoff_timer(executor)
            , m_rng(seed)
    {
    }

    void on_redial(plexus::detail::move_only_function<void()> cb)
    {
        m_on_redial = std::move(cb);
    }

    void on_dead(plexus::detail::move_only_function<void()> cb)
    {
        m_on_dead = std::move(cb);
    }

    // IDEMPOTENT while a dial is in flight: a second concurrent dial for the one slot would let
    // the later session build destroy the earlier channel while its handshake write is still
    // queued — a use-after-free on the write completion.
    void start()
    {
        if(m_dialing)
            return;
        m_first_attempt = Clock::now();
        dial();
    }

    void notify_dial_failed()
    {
        m_dialing = false;
        schedule_redial();
    }

    void mark_dial_settled() noexcept
    {
        m_dialing = false;
    }

    // A transport drop on an already-complete session. A clean/intentional close must NOT route
    // here — only a transport drop does, so a self-defending close never amplifies into a re-dial.
    void on_channel_dropped()
    {
        m_dialing = false;
        schedule_redial();
    }

    std::uint32_t attempt_count() const noexcept
    {
        return m_attempt;
    }
    bool is_surrendered() const noexcept
    {
        return m_surrendered;
    }

private:
    void schedule_redial()
    {
        ++m_attempt;
        if(surrendered())
            return report_dead();
        m_backoff_timer.expires_after(compute_backoff(m_cfg, m_attempt, m_rng));
        m_backoff_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(ec)
                        return;
                    dial();
                });
    }

    void dial()
    {
        m_dialing = true;
        if(m_on_redial)
            m_on_redial();
        m_transport.dial(m_endpoint);
    }

    bool surrendered() const noexcept
    {
        return (m_cfg.max_attempts.has_value() && m_attempt >= *m_cfg.max_attempts) || (m_cfg.max_elapsed.has_value() && elapsed() >= *m_cfg.max_elapsed);
    }

    std::chrono::milliseconds elapsed() const noexcept
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - m_first_attempt);
    }

    void report_dead()
    {
        m_surrendered = true;
        if(m_on_dead)
            m_on_dead();
    }

    Transport &m_transport;
    reconnect_config m_cfg;
    io::endpoint m_endpoint;
    timer_type m_backoff_timer;
    std::mt19937_64 m_rng;
    typename Clock::time_point m_first_attempt{};
    std::uint32_t m_attempt{0};
    bool m_surrendered{false};
    bool m_dialing{false};
    plexus::detail::move_only_function<void()> m_on_redial;
    plexus::detail::move_only_function<void()> m_on_dead;
};

}

#endif
