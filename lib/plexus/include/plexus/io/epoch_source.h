#ifndef HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H
#define HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H

#include <cstdint>

namespace plexus::io {

// The per-peer epoch well. It OUTLIVES any single peer_session incarnation: the
// owner (the harness today, the per-peer registry block tomorrow) holds one of
// these next to the connection slot and hands it by reference to each successive
// peer_session. Every incarnation mints its session_id epoch by drawing the next
// value here, so a reconnect's fresh session automatically gets a STRICTLY later
// epoch than the dead one — the cross-incarnation monotonicity the staleness gate
// relies on is structural, not stamped in by hand. uint8 wraps skipping 0 (a
// zero session_id is reserved for handshake control frames). A trivial, copyable,
// heap-free, global-free value type: MCU-friendly and embeddable in any slot.
class epoch_source
{
public:
    // Advance to the next epoch and return it. Wraps 255 -> 1, never yielding 0.
    std::uint8_t next() noexcept
    {
        ++m_counter;
        if(m_counter == 0)
            m_counter = 1;
        return m_counter;
    }

    std::uint8_t current() const noexcept { return m_counter; }

private:
    std::uint8_t m_counter{0};
};

}

#endif
