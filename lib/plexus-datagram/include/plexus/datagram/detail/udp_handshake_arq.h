#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_HANDSHAKE_ARQ_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_HANDSHAKE_ARQ_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::datagram::detail {

// A UDP session cannot establish over loss without retransmitting the handshake
// request/response, so this arms a fixed 250 / 500 / 1000 ms ladder (3 attempts). start() arms the
// first attempt and immediately sends — the first transmit is not on the timer, which covers only
// the retransmits.
template<typename Policy>
    requires plexus::Policy<Policy>
class udp_handshake_arq
{
public:
    using timer_type = typename Policy::timer_type;

    using schedule = std::array<std::chrono::milliseconds, 3>;

    // The delay before retransmit attempt N (the first transmit fires immediately at start, so
    // index 0 guards transmit #1).
    static constexpr schedule default_ladder{std::chrono::milliseconds{250}, std::chrono::milliseconds{500}, std::chrono::milliseconds{1000}};

    explicit udp_handshake_arq(typename Policy::executor_type executor, schedule ladder = default_ladder)
            : m_timer(executor)
            , m_ladder(ladder)
    {
    }

    udp_handshake_arq(const udp_handshake_arq &)            = delete;
    udp_handshake_arq &operator=(const udp_handshake_arq &) = delete;

    void on_transmit(plexus::detail::move_only_function<void()> cb)
    {
        m_on_transmit = std::move(cb);
    }

    void on_established(plexus::detail::move_only_function<void()> cb)
    {
        m_on_established = std::move(cb);
    }

    void on_timeout(plexus::detail::move_only_function<void()> cb)
    {
        m_on_timeout = std::move(cb);
    }

    void start()
    {
        if(m_resolved)
            return;
        transmit();
        arm();
    }

    static constexpr std::size_t max_attempts() noexcept
    {
        return std::tuple_size_v<schedule>;
    }

    // Idempotent: a duplicate/late arrival after resolution is a no-op.
    void on_paired_frame()
    {
        if(m_resolved)
            return;
        m_resolved = true;
        m_timer.cancel();
        if(m_on_established)
            m_on_established();
    }

    // Cancel a pending timer so a fire after the dial state dies is a no-op.
    void cancel()
    {
        m_resolved = true;
        m_timer.cancel();
    }

    std::uint8_t attempts() const noexcept
    {
        return m_attempt;
    }
    bool resolved() const noexcept
    {
        return m_resolved;
    }

private:
    // m_attempt counts transmits already sent (1 after start()); the ladder entry for the current
    // attempt is the deadline.
    void arm()
    {
        m_timer.expires_after(m_ladder[m_attempt - 1]);
        m_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(ec)
                        return; // cancelled by resolution/teardown
                    if(m_attempt >= m_ladder.size())
                        return surrender();
                    transmit();
                    arm();
                });
    }

    void transmit()
    {
        ++m_attempt;
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
