#ifndef HPP_GUARD_PLEXUS_CRYPTO_ANTI_REPLAY_WINDOW_H
#define HPP_GUARD_PLEXUS_CRYPTO_ANTI_REPLAY_WINDOW_H

#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::crypto {

// The sliding-window width (in slots) the datagram anti-replay filter tracks below
// the highest accepted sequence. Set from a recorded reproducible sweep
// (anti_replay_window_sweep_test) over {64, 256, 1024, 4096} at FRAGMENT scale: a
// large (4 MiB) message fragments into ~3500 sealed datagrams, and the worst-case
// reorder displacement is bounded by the in-flight datagram window (BDP / MTU) — ~177
// on an untuned loopback (rmem_default / 1200 B) and ~1042 on a 1 Gbps × 10 ms-RTT
// link. 64 and 256 false-reject below that realistic-link depth, and 1024 is clean to
// a displacement of ~1031 — just short of the ~1042 realistic-link worst case. 4096 is
// the smallest swept width that bears margin over the realistic-link in-flight window
// (clean well past 1042), covering the loopback and LAN regimes with a >20x margin, at
// a fixed 512-byte (64 × uint64) cost. A datagram displaced further back is treated as
// too-old and dropped — bounded O(1) state, never an unbounded seen-set (the IPsec/DTLS
// RFC 4303 shape).
constexpr std::size_t k_anti_replay_window_bits = 4096;

enum class replay_verdict : std::uint8_t
{
    accept,
    reject_replay,
    reject_old
};

// A fixed-width RFC 4303 (§3.4.3) sliding-window bitmap, reset per AEAD key epoch
// (RFC 9147). It tracks the highest accepted sequence and a bitmap of the Width
// preceding slots; the check is O(1) and the state is bounded — never an unbounded
// set of seen sequences. The receiver reconstructs the AEAD nonce from the sequence
// carried explicitly on each datagram, so drop/reorder never desyncs a counter.
template<std::size_t Width = k_anti_replay_window_bits>
class anti_replay_window
{
public:
    static constexpr std::size_t width = Width;

    // The verdict check_and_set would return for seq, computed WITHOUT mutating any
    // state. The datagram path probes this before authenticating a packet so a forged
    // replay/too-old sequence is rejected without sliding the window (RFC 4303 §3.4.3:
    // the ICV is verified before the replay window is advanced); check_and_set then
    // commits the slide only after a successful open.
    [[nodiscard]] replay_verdict would_accept(std::uint64_t seq) const noexcept
    {
        if(!m_seen_any)
            return replay_verdict::accept;
        if(seq > m_highest)
            return replay_verdict::accept;
        const std::uint64_t back = m_highest - seq;
        if(back >= Width)
            return replay_verdict::reject_old;
        if(test_bit(back))
            return replay_verdict::reject_replay;
        return replay_verdict::accept;
    }

    [[nodiscard]] replay_verdict check_and_set(std::uint64_t seq) noexcept
    {
        if(!m_seen_any)
        {
            m_seen_any = true;
            m_highest  = seq;
            set_bit(0);
            return replay_verdict::accept;
        }
        if(seq > m_highest)
        {
            slide(seq - m_highest);
            m_highest = seq;
            set_bit(0);
            return replay_verdict::accept;
        }
        const std::uint64_t back = m_highest - seq;
        if(back >= Width)
            return replay_verdict::reject_old;
        if(test_bit(back))
            return replay_verdict::reject_replay;
        set_bit(back);
        return replay_verdict::accept;
    }

    void reset() noexcept
    {
        m_bitmap.fill(0u);
        m_highest  = 0;
        m_seen_any = false;
    }

    [[nodiscard]] std::uint64_t highest() const noexcept
    {
        return m_highest;
    }

private:
    static constexpr std::size_t k_word_bits = 64;
    static constexpr std::size_t k_words     = (Width + k_word_bits - 1) / k_word_bits;

    // Bit b counts slots BELOW m_highest: bit 0 is m_highest itself, bit k is the
    // sequence m_highest - k.
    void set_bit(std::size_t b) noexcept
    {
        m_bitmap[b / k_word_bits] |= (1ull << (b % k_word_bits));
    }
    [[nodiscard]] bool test_bit(std::size_t b) const noexcept
    {
        return (m_bitmap[b / k_word_bits] & (1ull << (b % k_word_bits))) != 0u;
    }

    // RFC 4303 §3.4.3 sliding-window advance as a whole-word shift: bit k tracks
    // m_highest - k, so a forward advance moves every bit toward a higher index. The
    // displacement splits into a word_shift word move plus a bit_shift intra-word carry
    // (the bit_shift == 0 edge is a pure word move — no carry term, so >> (64 - 0) is
    // never evaluated).
    void slide(std::uint64_t by) noexcept
    {
        if(by >= Width)
        {
            m_bitmap.fill(0u);
            return;
        }
        const std::size_t word_shift = static_cast<std::size_t>(by) / k_word_bits;
        const std::size_t bit_shift  = static_cast<std::size_t>(by) % k_word_bits;
        for(std::size_t i = k_words; i-- > 0;)
        {
            const std::size_t src = i - word_shift;
            std::uint64_t     w   = (i >= word_shift) ? (m_bitmap[src] << bit_shift) : 0u;
            if(bit_shift != 0 && i > word_shift)
                w |= m_bitmap[src - 1] >> (k_word_bits - bit_shift);
            m_bitmap[i] = w;
        }
        mask_above_width();
    }

    // Width is currently a whole multiple of k_word_bits (no partial top word), but keep
    // the shift correct for a future non-multiple Width: clear any bit at or above Width
    // so a displaced word cannot admit a phantom slot past the window.
    void mask_above_width() noexcept
    {
        constexpr std::size_t top_bits = Width % k_word_bits;
        if constexpr(top_bits != 0)
            m_bitmap[k_words - 1] &= (1ull << top_bits) - 1u;
    }

    std::array<std::uint64_t, k_words> m_bitmap{};
    std::uint64_t                      m_highest{0};
    bool                               m_seen_any{false};
};

}

#endif
