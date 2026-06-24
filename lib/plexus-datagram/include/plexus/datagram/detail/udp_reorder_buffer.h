#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_REORDER_BUFFER_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_REORDER_BUFFER_H

#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::datagram::detail {

// The receiver-side in-order release buffer for the reliable data ARQ: it reorders discrete
// datagrams by a uint16 seq and emits them in publish order with head-of-line blocking (a gap at
// `expected` holds it and every higher seq until the gap fills, then drains the contiguous run).
// Wrap-safety: the comparison is RFC 1982 serial-number arithmetic (the forward distance mod 2^16
// splits the space in half), so a 65535 -> 0 rollover is a forward step, not a blackout. A seq
// below `expected` is a dropped duplicate; a seq at or beyond expected+W is out of window, dropped.
class udp_reorder_buffer
{
public:
    static constexpr std::uint16_t half_space   = 32768;
    static constexpr std::size_t default_window = 512;

    enum class outcome : std::uint8_t
    {
        delivered,    // fed seq was contiguous (drove an in-order release run)
        buffered,     // fed seq was ahead of a gap -> held (HOL)
        duplicate,    // fed seq was below expected -> already delivered, dropped
        out_of_window // fed seq was at/beyond expected+W -> dropped (bound enforced)
    };

    // CONTRACT: both ends start at the same initial_seq, so the cumulative-ack edge is meaningful
    // from segment one.
    explicit udp_reorder_buffer(std::size_t window = default_window, std::uint16_t initial_seq = 0)
            : m_window(window == 0 ? default_window : window)
            , m_slots(m_window)
            , m_expected(initial_seq)
    {
    }

    // The buffer calls the sink synchronously, strictly in publish order, exactly once per distinct
    // seq. The fragmented bit rides the slot so the channel routes a reliable fragment to the
    // reassembler on release.
    void on_deliver(plexus::detail::move_only_function<void(std::uint16_t, bool, std::span<const std::byte>)> cb)
    {
        m_on_deliver = std::move(cb);
    }

    outcome feed(std::uint16_t seq, bool fragmented, std::span<const std::byte> bytes)
    {
        auto adv = static_cast<std::uint16_t>(seq - m_expected);
        if(adv >= half_space)
            return outcome::duplicate; // behind expected: already delivered
        if(adv >= m_window)
            return outcome::out_of_window; // at/beyond the bound: drop, do not grow

        auto &slot         = m_slots[(m_base + adv) % m_window];
        const bool was_gap = (adv != 0);
        if(slot.present)
            return was_gap ? outcome::buffered : outcome::duplicate; // re-buffered hole / dup at edge

        slot.bytes.assign(bytes.begin(), bytes.end());
        slot.fragmented = fragmented;
        slot.present    = true;
        if(adv != 0)
            return outcome::buffered; // a hole remains at expected -> HOL hold

        drain_contiguous();
        return outcome::delivered;
    }

    // The highest in-order seq delivered so far (the cumulative-ack edge).
    std::uint16_t cumulative() const noexcept
    {
        return static_cast<std::uint16_t>(m_expected - 1);
    }

    std::uint16_t expected() const noexcept
    {
        return m_expected;
    }

    // Drives the selective-ack bitmap the receiver returns; hole 0 == expected (the gap itself).
    bool buffered_at(std::size_t hole) const noexcept
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
        bool fragmented{false};
    };

    // Release the run of contiguous present slots from the base until the next gap, advancing
    // expected and the ring base past each; the vacated slot's capacity is retained for reuse.
    void drain_contiguous()
    {
        while(m_slots[m_base].present)
        {
            auto &s = m_slots[m_base];
            if(m_on_deliver)
                m_on_deliver(m_expected, s.fragmented, std::span<const std::byte>{s.bytes});
            s.present = false;
            ++m_expected;
            m_base = (m_base + 1) % m_window;
        }
    }

    std::size_t m_window;
    std::vector<slot> m_slots;
    std::size_t m_base{0}; // ring index of `expected`
    std::uint16_t m_expected{0};
    plexus::detail::move_only_function<void(std::uint16_t, bool, std::span<const std::byte>)> m_on_deliver;
};

}

#endif
