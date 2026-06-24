#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_DEDUP_WINDOW_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_DEDUP_WINDOW_H

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

// Sliding-bitmap anti-replay window over a uint16 sequence. Tracks the highest
// seq seen plus a bitmap of the previous `depth` seq values. O(1) admit, no
// hot-path allocation (the fixed bitmap is the whole store). Per-peer state;
// reset() clears it on every handshake/session-id transition so a remote restart
// resets the receive-side window in lockstep with the local seq reset (otherwise
// the first post-handshake seq=0 would be judged stale against handshake-era
// state and silently dropped).
//
// admit() compares the wire seq against the high-water mark with serial-number
// (RFC 1982) arithmetic: the forward distance (seq - high_water) mod 2^16 splits
// the space in half, so a value in (0, 32768) is a NEWER datagram and a value in
// [32768, 65536) is OLDER. This makes the window wrap-agnostic — a uint16
// 65535 -> 0 rollover during continuous publishing is a forward step of 1, not a
// blackout — so no per-wrap reset is needed within a long-lived session. The
// width-agnostic logic is the proven uint8 window's; only the half-space constant
// widens from 128 (uint8 half) to 32768 (uint16 half).
//
// depth_max stays 32 (proven-core parity): the wider seq removes the UPPER
// coupling to depth (the reliable sliding window is a separate, larger knob), so
// the dedup window need not grow — it still only guards duplicate/reorder within a
// small recent span. The invariant "(1 << seq_bits) >= 4 * depth_max" — the
// disambiguation margin the modular comparison relies on — holds trivially.
class udp_dedup_window
{
public:
    using seq_t                             = std::uint16_t;
    static constexpr std::size_t depth_max  = 32;
    static constexpr seq_t       half_space = 32768;

    enum class outcome : std::uint8_t
    {
        fresh,
        duplicate,
        too_old
    };

    explicit udp_dedup_window(std::size_t depth = depth_max) noexcept
            : m_depth{std::clamp(depth, std::size_t{1}, depth_max)}
    {
    }

    [[nodiscard]] outcome admit(seq_t seq) noexcept
    {
        // Forward distance mod 2^16: in (0, half_space) -> seq is AHEAD (newer, transparently
        // including a 65535 -> 0 wrap); in [half_space, 2^16) -> seq is BEHIND. The
        // (1<<16) >= 4*depth_max invariant guarantees an in-window old datagram can never be
        // mistaken for a forward jump, so the wrap needs no reset.
        auto adv = static_cast<seq_t>(seq - m_high_water);
        if(adv != 0u && adv < half_space)
            return advance_to(seq, adv);

        auto delta = static_cast<std::size_t>(static_cast<seq_t>(m_high_water - seq));
        if(delta >= m_depth)
            return outcome::too_old;
        auto mask = 1ull << delta;
        if(m_bitmap & mask)
            return outcome::duplicate;
        m_bitmap |= mask;
        return outcome::fresh;
    }

private:
    // Shift the seen-bitmap by the forward advance (clearing it on a jump past the bitmap width),
    // set the just-seen bit, and re-anchor the high-water mark. The newest datagram is fresh.
    [[nodiscard]] outcome advance_to(seq_t seq, seq_t adv) noexcept
    {
        m_bitmap = adv >= 64u ? 0u : (m_bitmap << adv);
        m_bitmap |= 1ull;
        m_high_water = seq;
        return outcome::fresh;
    }

public:
    void reset() noexcept
    {
        m_high_water = 0;
        m_bitmap     = 0;
    }

    [[nodiscard]] std::size_t depth() const noexcept
    {
        return m_depth;
    }

private:
    std::size_t   m_depth;
    seq_t         m_high_water{0};
    std::uint64_t m_bitmap{0};
};

static_assert((1u << 16) >= 4u * udp_dedup_window::depth_max,
              "udp_dedup_window: seq width insufficient against depth_max "
              "(invariant: (1 << seq_bits) >= 4 * depth_max). uint16 (65536 distinct "
              "values) admits depth_max far beyond the 32 carried here.");

}

#endif
