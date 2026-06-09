#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PRIORITY_BAND_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PRIORITY_BAND_QUEUE_H

#include "plexus/io/priority.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <algorithm>

namespace plexus::io::detail {

// The fixed number of egress priority bands one destination is split into. Kept a
// small constant so the per-destination band cost is bounded; the exact count is to
// be substantiated at the fan-out benchmark, not fixed by feel.
constexpr std::size_t k_egress_bands = 4;

// The fixed per-band message capacity (a setup-time bound). The per-destination
// memory ceiling is k_egress_bands * k_band_depth * max_payload; the depth is to be
// swept at the fan-out benchmark rather than fixed by feel.
constexpr std::size_t k_band_depth = 256;

// Map a priority to a band index: realtime -> 0 (highest drain priority) ...
// background -> 3. The drain order is ascending band index = descending priority, so
// the inversion subtracts the enum value from the top band; the result is clamped to
// the last band for a hypothetical out-of-range value.
inline std::size_t band_of(priority p) noexcept
{
    const std::size_t inverted = k_egress_bands - 1 - static_cast<std::size_t>(p);
    return std::min(inverted, k_egress_bands - 1);
}

// One destination's N priority bands of pooled owned-node frame buffers. Each band
// is a fixed-depth FIFO ring whose slots are grown ONCE (cloning the history_ring
// grow-once / assign-into-slot pattern) and reused thereafter, so a steady
// enqueue/pop loop is allocation-free. A band at capacity DROPS THE NEWEST frame
// (refuses the new frame, bumps a counter, leaves the resident set untouched) — the
// fixed bounded-band overflow policy for this layer; it never branches on any
// congestion vocabulary, never evicts the oldest, never blocks. Single-owner pooled
// vectors only: no shared_ptr, no atomics, no shared_from_this.
class priority_band_queue
{
public:
    // True when any band holds a frame.
    [[nodiscard]] bool has_work() const noexcept
    {
        for(const auto &b : m_bands)
            if(!b.empty())
                return true;
        return false;
    }

    // Copy a framed buffer into the given band, returning false (admitting nothing)
    // when that band is at k_band_depth — the drop-newest backpressure edge.
    bool enqueue(std::size_t band, std::span<const std::byte> frame)
    {
        return m_bands[band].push(frame);
    }

    // The front node of the highest non-empty band (lowest index), or nullptr when no
    // band holds work. The node stays pool-resident until the caller copies it out and
    // then calls pop_highest — the in-flight-safe ordering.
    [[nodiscard]] const std::vector<std::byte> *front_highest() const
    {
        for(const auto &b : m_bands)
            if(!b.empty())
                return &b.front();
        return nullptr;
    }

    // Advance the FIFO tail of the highest non-empty band (the same band front_highest
    // returned). A no-op when every band is empty.
    void pop_highest()
    {
        for(auto &b : m_bands)
            if(!b.empty())
                return b.pop();
    }

    [[nodiscard]] std::size_t dropped_newest_count(std::size_t band) const noexcept
    {
        return m_bands[band].dropped_newest();
    }

private:
    // A single band: a fixed-depth FIFO ring of pooled owned-node buffers grown once
    // and reused via assign, with a drop-newest-at-capacity admission.
    struct band
    {
        [[nodiscard]] bool empty() const noexcept { return m_count == 0; }

        // Admit into the slot one-past the tail, growing the ring to k_band_depth on
        // first touch; refuse + count when the band already holds k_band_depth frames.
        bool push(std::span<const std::byte> frame)
        {
            if(m_count == k_band_depth)
            {
                ++m_dropped_newest;
                return false;
            }
            if(m_slots.size() != k_band_depth)
                m_slots.resize(k_band_depth);
            const std::size_t tail = (m_head + m_count) % k_band_depth;
            m_slots[tail].assign(frame.begin(), frame.end());
            ++m_count;
            return true;
        }

        [[nodiscard]] const std::vector<std::byte> &front() const { return m_slots[m_head]; }

        void pop() noexcept
        {
            m_head = (m_head + 1) % k_band_depth;
            --m_count;
        }

        [[nodiscard]] std::size_t dropped_newest() const noexcept { return m_dropped_newest; }

        std::vector<std::vector<std::byte>> m_slots;
        std::size_t m_head{0};
        std::size_t m_count{0};
        std::size_t m_dropped_newest{0};
    };

    std::array<band, k_egress_bands> m_bands;
};

}

#endif
