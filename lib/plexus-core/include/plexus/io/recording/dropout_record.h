#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_DROPOUT_RECORD_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_DROPOUT_RECORD_H

#include "plexus/io/capture_policy.h"

#include <cstdint>

namespace plexus::io::recording {

// The recorder's honest account of its OWN loss: when the ring sheds a record it
// could not admit, the gap is quantified here rather than dropped silently. ts is
// when capture resumed (the first admit after the run); count and bytes sum the
// shed records; max_fidelity is the highest fidelity in the gap, bounding what was
// lost. Distinct from a drop_event (a network/QoS message loss the recorder logs
// as ordinary data) — this is the ring overflowing, and it composes with it.
struct dropout_record
{
    std::uint64_t    ts{};
    std::uint32_t    count{};
    std::uint64_t    bytes{};
    capture_fidelity max_fidelity{capture_fidelity::off};
};

// Accumulates a contiguous run of shed records on the producer side. Every failed
// admit bumps the run; the next successful admit harvests it into a dropout_record
// and resets, so the surfaced count is exact by construction (recall 1.0) — the
// producer cannot shed a record without incrementing the run.
class dropout_run
{
public:
    void shed(std::uint64_t would_be_bytes, capture_fidelity fidelity) noexcept
    {
        ++m_count;
        m_bytes += would_be_bytes;
        if(fidelity > m_max_fidelity)
            m_max_fidelity = fidelity;
    }

    [[nodiscard]] bool pending() const noexcept { return m_count != 0; }

    dropout_record harvest(std::uint64_t resumed_ts) noexcept
    {
        const dropout_record out{resumed_ts, m_count, m_bytes, m_max_fidelity};
        m_count        = 0;
        m_bytes        = 0;
        m_max_fidelity = capture_fidelity::off;
        return out;
    }

private:
    std::uint32_t    m_count{0};
    std::uint64_t    m_bytes{0};
    capture_fidelity m_max_fidelity{capture_fidelity::off};
};

}

#endif
