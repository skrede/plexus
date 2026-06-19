#ifndef HPP_GUARD_PLEXUS_IO_RECONNECT_H
#define HPP_GUARD_PLEXUS_IO_RECONNECT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/detail/compat.h"

#include "plexus/policy.h"

#include <chrono>
#include <random>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::io {

// Full-jitter exponential backoff with a ceiling, the proven thundering-herd-safe
// shape: delay = uniform(0, min(max_delay, min_delay * 2^attempt)). The RNG is
// injected so the deterministic oracle fixes the seed for a reproducible backoff
// sequence and an MCU port can swap the PRNG (the <random> dependency stays behind
// this seam — never a bare random_device{}() inline). The shift is capped at 20 so
// the 64-bit scale cannot overflow.
template<typename Rng>
std::chrono::milliseconds compute_backoff(const reconnect_config &cfg, std::uint32_t attempt,
                                          Rng &rng)
{
    const auto shift   = std::min(attempt, std::uint32_t{20});
    auto       ceiling = cfg.min_delay * (std::uint64_t{1} << shift);
    if(ceiling > cfg.max_delay)
        ceiling = cfg.max_delay;
    std::uniform_int_distribution<std::int64_t> dist{0, static_cast<std::int64_t>(ceiling.count())};
    return std::chrono::milliseconds{dist(rng)};
}

// The single-connection reconnect driver. It lives OUTSIDE peer_session
// (single-connection-agnostic, so the future registry owns it multi-peer
// unchanged): the harness owns the peer_session and rebuilds it from the channel
// transport.on_dialed delivers; the driver owns only the dial-retry cycle. Two
// triggers: an initial/continued dial-failure (the transport's on_dial_failed,
// which is also where the FSM's dialing→retry intent lands) and an established
// session's channel drop (on_channel_dropped, called by the harness on a transport
// drop — NOT on a clean/intentional close, so a self-defending close never
// amplifies into a re-dial). Each scheduled re-dial arms a Policy timer for a
// full-jitter backoff; the handler self-guards if(ec) so a timer firing after
// teardown is a no-op. Surrender bounds (max_attempts/max_elapsed) stop the cycle
// and latch is_surrendered(); the harness rebuilds a fresh epoch on each on_dialed.
template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy>
class reconnect
{
public:
    using timer_type = typename Policy::timer_type;

    reconnect(Transport &transport, typename Policy::executor_type executor,
              const reconnect_config &cfg, io::endpoint endpoint, std::uint64_t seed = 0)
            : m_transport(transport)
            , m_cfg(cfg)
            , m_endpoint(std::move(endpoint))
            , m_backoff_timer(executor)
            , m_rng(seed)
    {
    }

    // Fired just before each dial so the harness can tear down a dead incarnation
    // (the established-drop path) before the fresh channel arrives via on_dialed.
    void on_redial(plexus::detail::move_only_function<void()> cb) { m_on_redial = std::move(cb); }

    // Fired when a surrender bound is crossed: the session is reported dead and the
    // driver stops re-dialing.
    void on_dead(plexus::detail::move_only_function<void()> cb) { m_on_dead = std::move(cb); }

    // Begin the first dial. The first-attempt timestamp is read from the same Clock
    // the backoff timer uses so max_elapsed is provable on the virtual clock. The
    // dial-failure trigger is NOT self-wired on the transport: many drivers share one
    // transport (the multi-peer registry), so a single transport callback cannot
    // belong to one driver. The OWNER routes the transport's per-endpoint failure to
    // THIS driver's notify_dial_failed() — for a known endpoint, that is unambiguous.
    // Begin the first dial — IDEMPOTENT while a dial is already in flight. The owner
    // may call start() repeatedly off independent stimuli (a discovery browse that
    // re-resolves the same peer, an eager note_peer that re-fires) before the first
    // dial settles; without this guard each call opens a SECOND concurrent connection
    // for the one slot, and the later session build destroys the earlier channel while
    // its handshake write is still queued in the reactor — a use-after-free on the
    // write completion. One in-flight dial per driver; the outcome (settled / failed /
    // dropped) re-opens the gate.
    void start()
    {
        if(m_dialing)
            return;
        m_first_attempt = Clock::now();
        dial();
    }

    // The owner observed a dial failure for THIS driver's endpoint: back off and
    // re-dial. (The registry/engine correlates the transport's per-endpoint failure
    // to the matching slot; a single-connection owner routes its sole failure here.)
    void notify_dial_failed()
    {
        m_dialing = false;
        schedule_redial();
    }

    // The dial produced a channel (the engine's on_dialed tail built — or will build —
    // the session): the in-flight gate re-opens so a later drop can re-dial. Connected
    // peers are additionally short-circuited by the engine's is_connected guard.
    void mark_dial_settled() noexcept { m_dialing = false; }

    // An established session's transport dropped (broken_pipe/connection_reset on an
    // already-complete session). Back off and re-dial a fresh incarnation. A clean
    // tear_down/intentional close must NOT route here — only a transport drop does.
    void on_channel_dropped()
    {
        m_dialing = false;
        schedule_redial();
    }

    std::uint32_t attempt_count() const noexcept { return m_attempt; }
    bool          is_surrendered() const noexcept { return m_surrendered; }

private:
    // A retry is committed here (the attempt counter advances at scheduling time so
    // a scheduled-but-not-yet-fired re-dial is observable): check the surrender
    // bounds, then arm the backoff timer. The handler self-guards if(ec) so a timer
    // firing after teardown is a no-op.
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
                        return; // cancelled by teardown — never dial a dead driver
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
        return (m_cfg.max_attempts.has_value() && m_attempt >= *m_cfg.max_attempts) ||
                (m_cfg.max_elapsed.has_value() && elapsed() >= *m_cfg.max_elapsed);
    }

    std::chrono::milliseconds elapsed() const noexcept
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                     m_first_attempt);
    }

    void report_dead()
    {
        m_surrendered = true;
        if(m_on_dead)
            m_on_dead();
    }

    Transport                                 &m_transport;
    reconnect_config                           m_cfg;
    io::endpoint                               m_endpoint;
    timer_type                                 m_backoff_timer;
    std::mt19937_64                            m_rng;
    typename Clock::time_point                 m_first_attempt{};
    std::uint32_t                              m_attempt{0};
    bool                                       m_surrendered{false};
    bool                                       m_dialing{false};
    plexus::detail::move_only_function<void()> m_on_redial;
    plexus::detail::move_only_function<void()> m_on_dead;
};

}

#endif
