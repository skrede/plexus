#ifndef HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H
#define HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H

#include <cstdint>
#include <type_traits>

namespace plexus::io {

// The per-peer epoch well. It OUTLIVES any single peer_session incarnation: the
// owner (the harness today, the per-peer registry block tomorrow) holds one of
// these next to the connection slot and hands it by reference to each successive
// peer_session. Every incarnation mints its session_id epoch by drawing the next
// value here, so a reconnect's fresh session automatically gets a STRICTLY later
// epoch than the dead one — the cross-incarnation monotonicity the staleness gate
// relies on is structural, not stamped in by hand. The u64 counter wraps skipping 0
// (a zero session_id is reserved for handshake control frames); the width is wide
// enough that the wrap is unreachable in practice, retiring the u8 255-reconnect
// collision window. A trivial, copyable, heap-free, global-free value type:
// MCU-friendly and embeddable in any slot.
class epoch_source
{
public:
    // Advance to the next epoch and return it. Wraps u64-max -> 1, never yielding 0.
    std::uint64_t next() noexcept
    {
        ++m_counter;
        if(m_counter == 0)
            m_counter = 1;
        return m_counter;
    }

    std::uint64_t current() const noexcept
    {
        return m_counter;
    }

private:
    std::uint64_t m_counter{0};
};

// Pin the mint width: the epoch feeds frame_header.session_id (u64), and a silent
// narrowing here would reintroduce the wrap collision the widening retired.
static_assert(std::is_same_v<decltype(epoch_source{}.next()), std::uint64_t>, "epoch mint width must stay u64 so the session_id wire field never narrows");

}

#endif
