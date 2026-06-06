#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_HANDSHAKE_ARQ_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_HANDSHAKE_ARQ_H

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::asio::detail {

// The handshake ARQ: a UDP session cannot establish over loss without retransmitting
// the handshake request/response, so this arms a fixed 250 / 500 / 1000 ms ladder
// (3 attempts) — a port of the proven-core schedule, RESHAPED to plexus's
// single-owner lifetime. It composes reconnect.h's timer/retry/surrender skeleton
// (arm a Policy timer -> async_wait([this](ec){ if(ec) return; }); the attempt
// counter advances at arm time; a surrender bound latches and reports) with the
// handshake_fsm pure-action split (the ARQ moves no bytes — it calls an
// owner-installed retransmit/established/timeout action).
//
// LIFETIME (the single biggest port-vs-reshape boundary): the proven core captured
// shared_from_this into the timer and bounced through a strand. plexus FORBIDS both.
// This captures bare `this`, single-owner; the owning udp_transport cancels the timer
// (the dtor / cancel()) before the per-peer dial state dies, so a fire after teardown
// is a cancelled no-op (ec == operation_aborted -> the if(ec) guard returns). No
// shared_from_this, no strand, no static singleton.
//
// start() arms the first attempt and immediately sends (the first transmit is not on
// the timer — the timer covers the RETRANSMITS). on_established() (fed by the demux
// recv path when the paired frame arrives) cancels and completes exactly once.
template <typename Policy>
    requires plexus::Policy<Policy>
class udp_handshake_arq
{
public:
    using timer_type = typename Policy::timer_type;

    using schedule = std::array<std::chrono::milliseconds, 3>;

    // The fixed retransmit ladder: the delay BEFORE retransmit attempt N (the first
    // transmit fires immediately at start, so index 0 = 250 ms guards transmit #1).
    // The proven-core schedule and the production default.
    static constexpr schedule default_ladder{
        std::chrono::milliseconds{250},
        std::chrono::milliseconds{500},
        std::chrono::milliseconds{1000}};

    // The ladder is a required-WITH-default ctor argument: production binds the proven
    // 250/500/1000 ms schedule; a deterministic test may bind a compressed ladder to
    // exercise the same retransmit/surrender mechanics quickly. NOT a mutable setter —
    // the schedule is fixed at construction, never a test-only post-hoc knob.
    explicit udp_handshake_arq(typename Policy::executor_type executor, schedule ladder = default_ladder)
        : m_timer(executor)
        , m_ladder(ladder)
    {
    }

    udp_handshake_arq(const udp_handshake_arq &) = delete;
    udp_handshake_arq &operator=(const udp_handshake_arq &) = delete;

    // Called on each ladder fire: (re)send the handshake frame.
    void on_transmit(plexus::detail::move_only_function<void()> cb) { m_on_transmit = std::move(cb); }
    // Called once when the paired frame arrives: the session is established.
    void on_established(plexus::detail::move_only_function<void()> cb) { m_on_established = std::move(cb); }
    // Called once when all 3 attempts are exhausted: a handshake-timeout abort.
    void on_timeout(plexus::detail::move_only_function<void()> cb) { m_on_timeout = std::move(cb); }

    // Send the first handshake frame (attempt #1) and arm the wait for its response.
    void start()
    {
        if(m_resolved)
            return;
        transmit();
        arm();
    }

    [[nodiscard]] static constexpr std::size_t max_attempts() noexcept { return std::tuple_size_v<schedule>; }

    // The paired handshake frame arrived: cancel the pending retransmit and complete.
    // Idempotent — a duplicate/late arrival after resolution is a no-op.
    void on_paired_frame()
    {
        if(m_resolved)
            return;
        m_resolved = true;
        m_timer.cancel();
        if(m_on_established)
            m_on_established();
    }

    // The owner is tearing the peer down: cancel a pending timer so a fire after the
    // dial state dies is a no-op (the single-owner discipline).
    void cancel()
    {
        m_resolved = true;
        m_timer.cancel();
    }

    [[nodiscard]] std::uint8_t attempts() const noexcept { return m_attempt; }
    [[nodiscard]] bool resolved() const noexcept { return m_resolved; }

private:
    // Arm the wait that guards the transmit just sent. The ladder entry for the
    // CURRENT attempt is the deadline; when it elapses we either retransmit (an
    // attempt remains) or surrender (the last attempt's wait elapsed with no paired
    // frame). m_attempt counts transmits already sent (1 after start()).
    void arm()
    {
        m_timer.expires_after(m_ladder[m_attempt - 1]);
        m_timer.async_wait([this](std::error_code ec)
        {
            if(ec)
                return;                  // cancelled by resolution/teardown — never fire on a dead ARQ
            if(m_attempt >= m_ladder.size())
                return surrender();      // the final attempt's wait elapsed: abort, no further transmit
            transmit();
            arm();
        });
    }

    void transmit()
    {
        ++m_attempt;                     // count this transmit (1 after start, up to ladder size)
        if(m_on_transmit)
            m_on_transmit();
    }

    void surrender()
    {
        if(m_resolved)
            return;
        m_resolved = true;
        if(m_on_timeout)
            m_on_timeout();
    }

    timer_type m_timer;
    schedule m_ladder;
    std::uint8_t m_attempt{0};
    bool m_resolved{false};
    plexus::detail::move_only_function<void()> m_on_transmit;
    plexus::detail::move_only_function<void()> m_on_established;
    plexus::detail::move_only_function<void()> m_on_timeout;
};

}

#endif
