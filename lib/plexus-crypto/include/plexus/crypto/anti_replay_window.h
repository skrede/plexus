#ifndef HPP_GUARD_PLEXUS_CRYPTO_ANTI_REPLAY_WINDOW_H
#define HPP_GUARD_PLEXUS_CRYPTO_ANTI_REPLAY_WINDOW_H

#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::crypto {

// The sliding-window width (in slots) the datagram anti-replay filter tracks below
// the highest accepted sequence. Set from a recorded reproducible sweep
// (anti_replay_window_sweep_test) over {32, 64, 128, 256} under a modeled UDP
// reorder/loss distribution: 64 is the smallest width whose false-reject rate is
// zero at a reorder depth of 32 (a 2x margin to the modeled worst case) at a fixed
// 8-byte (one std::uint64_t) memory cost. A datagram displaced further back than
// this many slots is treated as too-old and dropped — bounded O(1) state, never an
// unbounded seen-set (the IPsec/DTLS RFC 4303 shape).
constexpr std::size_t k_anti_replay_window_bits = 64;

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
template <std::size_t Width = k_anti_replay_window_bits>
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
            m_highest = seq;
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
        m_highest = 0;
        m_seen_any = false;
    }

    [[nodiscard]] std::uint64_t highest() const noexcept { return m_highest; }

private:
    static constexpr std::size_t k_word_bits = 64;
    static constexpr std::size_t k_words = (Width + k_word_bits - 1) / k_word_bits;

    // Bit b counts slots BELOW m_highest: bit 0 is m_highest itself, bit k is the
    // sequence m_highest - k.
    void set_bit(std::size_t b) noexcept { m_bitmap[b / k_word_bits] |= (1ull << (b % k_word_bits)); }
    [[nodiscard]] bool test_bit(std::size_t b) const noexcept
    {
        return (m_bitmap[b / k_word_bits] & (1ull << (b % k_word_bits))) != 0u;
    }

    void slide(std::uint64_t by) noexcept
    {
        if(by >= Width)
        {
            m_bitmap.fill(0u);
            return;
        }
        for(std::size_t b = Width; b-- > 0;)
        {
            const bool was = (b >= by) && test_bit(b - by);
            assign_bit(b, was);
        }
    }

    void assign_bit(std::size_t b, bool v) noexcept
    {
        const std::uint64_t mask = 1ull << (b % k_word_bits);
        if(v)
            m_bitmap[b / k_word_bits] |= mask;
        else
            m_bitmap[b / k_word_bits] &= ~mask;
    }

    std::array<std::uint64_t, k_words> m_bitmap{};
    std::uint64_t m_highest{0};
    bool m_seen_any{false};
};

}

#endif
