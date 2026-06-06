#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_REORDER_BUFFER_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_REORDER_BUFFER_H

#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::asio::detail {

// The receiver-side in-order release buffer for the reliable data ARQ: it reorders
// DISCRETE datagrams by a uint16 seq and emits them in PUBLISH ORDER with head-of-line
// blocking (D-04). A gap at `expected` holds it AND every higher seq until the gap
// fills (by a retransmit), then drains the contiguous run. Shape-analog of the stream
// reassembler (hold input, emit complete units in order via a single callback, bounded
// with an overflow signal) but it reorders whole datagrams rather than concatenating a
// byte stream.
//
// Wrap-safety: the seq space is uint16 and the comparison is RFC-1982 serial-number
// arithmetic (the forward distance mod 2^16 splits the space in half), so a 65535 -> 0
// rollover during continuous delivery is a forward step, not a blackout — the same
// width-agnostic logic the dedup window uses. The buffer is bounded to a configured
// window W and allocated at setup (the slot ring is sized once in the ctor): no
// steady-state hot-path allocation. A seq below `expected` is a duplicate (already
// delivered) and dropped; a seq at or beyond expected+W is out of window and dropped.
//
// Single-owner, header-only, sans-IO: feed() drives an owner-installed on_deliver in
// order; the channel wraps the asio post around that callback (the buffer never touches
// the io_context).
class udp_reorder_buffer
{
public:
    static constexpr std::uint16_t half_space = 32768;
    static constexpr std::size_t default_window = 512;

    enum class outcome : std::uint8_t
    {
        delivered,       // fed seq was contiguous (drove an in-order release run)
        buffered,        // fed seq was ahead of a gap -> held (HOL)
        duplicate,       // fed seq was below expected -> already delivered, dropped
        out_of_window    // fed seq was at/beyond expected+W -> dropped (bound enforced)
    };

    // The initial expected seq is a STRUCTURAL ctor argument (defaulting to 0), NOT an
    // after-the-fact setter: production binds 0 on both ends (the sender's m_next and
    // this receiver's m_expected both start at 0 — the DOCUMENTED contract that makes the
    // cumulative-ack edge meaningful from segment one, since the handshake negotiates no
    // initial sequence). A test that exercises the uint16 wrap binds a non-zero start here.
    explicit udp_reorder_buffer(std::size_t window = default_window, std::uint16_t initial_seq = 0)
        : m_window(window == 0 ? default_window : window)
        , m_slots(m_window)
        , m_expected(initial_seq)
    {
    }

    // The owner-installed in-order delivery sink. The buffer calls it synchronously,
    // strictly in publish order, exactly once per distinct seq. The channel posts on
    // on_data around it (the buffer is sans-IO).
    void on_deliver(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> cb)
    {
        m_on_deliver = std::move(cb);
    }

    // Feed a received segment. seq == expected -> deliver it then drain contiguous
    // buffered successors (the gap-fill release). seq ahead of a gap -> buffer it and
    // every higher seq (HOL: nothing past the gap is released). seq behind expected ->
    // duplicate, dropped. seq at/beyond expected+W -> out of window, dropped.
    outcome feed(std::uint16_t seq, std::span<const std::byte> bytes)
    {
        auto adv = static_cast<std::uint16_t>(seq - m_expected);
        if(adv >= half_space)
            return outcome::duplicate;          // behind expected: already delivered
        if(adv >= m_window)
            return outcome::out_of_window;       // at/beyond the bound: drop, do not grow

        auto &slot = m_slots[(m_base + adv) % m_window];
        const bool was_gap = (adv != 0);
        if(slot.present)
            return was_gap ? outcome::buffered : outcome::duplicate;  // re-buffered hole / dup at edge

        slot.bytes.assign(bytes.begin(), bytes.end());
        slot.present = true;
        if(adv != 0)
            return outcome::buffered;             // a hole remains at expected -> HOL hold

        drain_contiguous();
        return outcome::delivered;
    }

    // The highest in-order seq delivered so far (expected - 1), the cumulative-ack edge.
    [[nodiscard]] std::uint16_t cumulative() const noexcept
    {
        return static_cast<std::uint16_t>(m_expected - 1);
    }

    [[nodiscard]] std::uint16_t expected() const noexcept { return m_expected; }

    // Is the slot at offset `hole` (above the cumulative edge) buffered? Drives the
    // selective-ack bitmap the receiver returns. hole 0 == expected (the gap itself).
    [[nodiscard]] bool buffered_at(std::size_t hole) const noexcept
    {
        if(hole >= m_window)
            return false;
        return m_slots[(m_base + hole) % m_window].present;
    }

private:
    struct slot
    {
        std::vector<std::byte> bytes;
        bool present{false};
    };

    // Release the run of contiguous present slots starting at the base, advancing
    // expected and the ring base past each, until the next gap. Each release fires the
    // in-order sink. The vacated slot is marked free (its capacity is retained for
    // reuse — no per-release free).
    void drain_contiguous()
    {
        while(m_slots[m_base].present)
        {
            auto &s = m_slots[m_base];
            if(m_on_deliver)
                m_on_deliver(m_expected, std::span<const std::byte>{s.bytes});
            s.present = false;
            ++m_expected;
            m_base = (m_base + 1) % m_window;
        }
    }

    std::size_t m_window;
    std::vector<slot> m_slots;            // a ring of W slots, allocated at setup
    std::size_t m_base{0};                // ring index of `expected`
    std::uint16_t m_expected{0};          // the next seq to deliver in order
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_deliver;
};

}

#endif
