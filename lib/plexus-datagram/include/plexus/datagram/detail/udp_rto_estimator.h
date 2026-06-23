#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_RTO_ESTIMATOR_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_RTO_ESTIMATOR_H

#include <chrono>
#include <algorithm>

namespace plexus::datagram::detail {

// The adaptive retransmit-timeout estimator (RFC 6298). It maintains the smoothed
// round-trip time SRTT and its variation RTTVAR from unambiguous round-trip samples and
// derives RTO = SRTT + 4*RTTVAR, clamped to [min, max]. The CALLER enforces Karn's
// algorithm (it never feeds a sample drawn from a retransmitted segment's ack, since
// such an ack is ambiguous about which transmission it acknowledges) — the estimator
// only owns the smoothing math, so it is unit-testable in isolation without a timer or a
// socket. A self-contained value type: no asio, no allocation.
class udp_rto_estimator
{
public:
    using ms = std::chrono::milliseconds;

    udp_rto_estimator(ms initial, ms min_rto, ms max_rto) noexcept
            : m_min(min_rto)
            , m_max(max_rto)
            , m_rto(initial)
    {
    }

    // Fold one unambiguous round-trip sample into SRTT/RTTVAR and recompute the RTO.
    // The first sample seeds SRTT = R, RTTVAR = R/2 (RFC 6298 §2.2); each subsequent
    // sample applies the standard 1/8 and 1/4 EWMA gains (§2.3).
    void sample(ms rtt) noexcept
    {
        if(!m_seeded)
        {
            m_srtt   = rtt;
            m_rttvar = rtt / 2;
            m_seeded = true;
        }
        else
        {
            const ms err = m_srtt > rtt ? m_srtt - rtt : rtt - m_srtt;
            m_rttvar     = (m_rttvar * 3 + err) / 4; // (1-1/4)*RTTVAR + 1/4*|SRTT-R|
            m_srtt       = (m_srtt * 7 + rtt) / 8;   // (1-1/8)*SRTT  + 1/8*R
        }
        m_rto = std::clamp(m_srtt + m_rttvar * 4, m_min, m_max);
    }

    // The current base RTO (before any per-segment retransmit backoff).
    [[nodiscard]] ms rto() const noexcept { return m_rto; }

    // The backed-off RTO for a segment retransmitted `retransmits` times: the base RTO
    // doubled per prior retransmit (Karn's multiplicative backoff), capped at max.
    [[nodiscard]] ms backed_off(unsigned retransmits) const noexcept
    {
        ms r = m_rto;
        for(unsigned i = 0; i < retransmits; ++i)
            r = std::min(r * 2, m_max);
        return r;
    }

    [[nodiscard]] bool seeded() const noexcept { return m_seeded; }

private:
    ms   m_min;
    ms   m_max;
    ms   m_rto;
    ms   m_srtt{0};
    ms   m_rttvar{0};
    bool m_seeded{false};
};

}

#endif
