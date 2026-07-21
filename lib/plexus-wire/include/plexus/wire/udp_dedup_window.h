#ifndef HPP_GUARD_PLEXUS_WIRE_UDP_DEDUP_WINDOW_H
#define HPP_GUARD_PLEXUS_WIRE_UDP_DEDUP_WINDOW_H

#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::wire {

// Sliding-bitmap anti-replay window over a uint16 sequence: the highest seq seen plus a bitmap
// of the previous `depth` seq values. O(1) admit, no hot-path allocation. reset() clears it on
// every handshake/session-id transition so a remote restart resets the receive-side window in
// lockstep with the local seq reset.
//
// The FIRST admit into a fresh (or reset) window anchors the high-water mark on whatever seq
// arrives, rather than measuring it against the default 0: without this a first seq in the OLDER
// half-space [32768, 65536) would classify too_old and never anchor, so a late-joining receiver
// (a per-origin counter already past the half-space) or a peer that restarts its counter would
// never advance its window off 0. reset() re-arms this first-sighting anchor so a session-id
// transition re-latches on the peer's post-restart seq.
//
// admit() compares a subsequent wire seq against the high-water mark with serial-number (RFC 1982)
// arithmetic: the forward distance (seq - high_water) mod 2^16 splits the space in half, so a
// value in (0, 32768) is NEWER and one in [32768, 65536) is OLDER. This makes the window
// wrap-agnostic — a 65535 -> 0 rollover is a forward step of 1, not a blackout — so no per-wrap
// reset is needed. The disambiguation margin holds via "(1 << seq_bits) >= 4 * depth_max".
class udp_dedup_window
{
public:
    using seq_t                            = std::uint16_t;
    static constexpr std::size_t depth_max = 32;
    static constexpr seq_t half_space      = 32768;

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

    outcome admit(seq_t seq) noexcept
    {
        if(!m_anchored)
        {
            anchor(seq);
            return outcome::fresh;
        }
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

    // Latch the window onto a known-good seq (a first sighting), bypassing the serial-number compare
    // that a default high-water of 0 would otherwise fail for a seq in the OLDER half-space.
    void anchor(seq_t seq) noexcept
    {
        m_high_water = seq;
        m_bitmap     = 1;
        m_anchored   = true;
    }

    void reset() noexcept
    {
        m_high_water = 0;
        m_bitmap     = 0;
        m_anchored   = false;
    }

    std::size_t depth() const noexcept
    {
        return m_depth;
    }

private:
    std::size_t m_depth;
    seq_t m_high_water{0};
    std::uint64_t m_bitmap{0};
    bool m_anchored{false};

    // Shift the seen-bitmap by the forward advance (clearing it on a jump past the bitmap width),
    // set the just-seen bit, and re-anchor the high-water mark.
    outcome advance_to(seq_t seq, seq_t adv) noexcept
    {
        m_bitmap = adv >= 64u ? 0u : (m_bitmap << adv);
        m_bitmap |= 1ull;
        m_high_water = seq;
        return outcome::fresh;
    }
};

static_assert((1u << 16) >= 4u * udp_dedup_window::depth_max,
              "udp_dedup_window: seq width insufficient against depth_max "
              "(invariant: (1 << seq_bits) >= 4 * depth_max)");

}

#endif
