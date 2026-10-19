#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_TALLY_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_TALLY_H

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>

namespace plexus::testing {

// Large enough that a few hundred messages fill a socket the reader does not drain, small enough for one ring slot.
inline constexpr std::size_t k_hold_payload_bytes = 3000;

// Marks a warm-up message, which no tally counts.
inline constexpr std::uint32_t k_hold_warm_bit = 0x80000000u;

inline std::int64_t hold_now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

inline void write_hold_payload(std::vector<std::byte> &out, std::uint32_t index, std::int64_t stamp_ns)
{
    out.assign(k_hold_payload_bytes, std::byte{0});
    std::memcpy(out.data(), &index, sizeof(index));
    std::memcpy(out.data() + sizeof(index), &stamp_ns, sizeof(stamp_ns));
}

// The nearest-rank percentile of samples, which it sorts.
inline std::int64_t percentile(std::vector<std::int64_t> &samples, double q)
{
    if(samples.empty())
        return 0;
    std::ranges::sort(samples);
    const auto rank = static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1) + 0.5);
    return samples[std::min(rank, samples.size() - 1)];
}

// One subscriber's view of a run: what it received, in which order, and how late. Latency and gaps count only the
// messages from measured_from on, since the earlier ones were published while the subscriber was not being run.
class hold_tally
{
public:
    explicit hold_tally(std::uint32_t total, std::uint32_t measured_from)
            : m_measured_from(measured_from)
            , m_delivered(0)
            , m_duplicates(0)
            , m_out_of_order(0)
            , m_highest(-1)
            , m_last_ns(0)
            , m_max_gap_ns(0)
            , m_seen(total, false)
            , m_latency_ns()
    {
    }

    void on_delivery(std::span<const std::byte> bytes)
    {
        std::uint32_t index    = 0;
        std::int64_t stamp_ns  = 0;
        const std::int64_t now = hold_now_ns();
        std::memcpy(&index, bytes.data(), sizeof(index));
        std::memcpy(&stamp_ns, bytes.data() + sizeof(index), sizeof(stamp_ns));
        if((index & k_hold_warm_bit) != 0 || index >= m_seen.size())
            return;
        count(index);
        if(index >= m_measured_from)
            time(now, stamp_ns);
    }

    std::uint32_t delivered() const noexcept
    {
        return m_delivered;
    }

    std::uint32_t duplicates() const noexcept
    {
        return m_duplicates;
    }

    std::uint32_t out_of_order() const noexcept
    {
        return m_out_of_order;
    }

    std::int64_t max_gap_ns() const noexcept
    {
        return m_max_gap_ns;
    }

    std::vector<std::int64_t> latencies() const
    {
        return m_latency_ns;
    }

private:
    std::uint32_t m_measured_from;
    std::uint32_t m_delivered;
    std::uint32_t m_duplicates;
    std::uint32_t m_out_of_order;
    std::int64_t m_highest;
    std::int64_t m_last_ns;
    std::int64_t m_max_gap_ns;
    std::vector<bool> m_seen;
    std::vector<std::int64_t> m_latency_ns;

    void count(std::uint32_t index)
    {
        if(m_seen[index])
            ++m_duplicates;
        else
            ++m_delivered;
        m_seen[index] = true;
        if(static_cast<std::int64_t>(index) < m_highest)
            ++m_out_of_order;
        m_highest = std::max<std::int64_t>(m_highest, index);
    }

    void time(std::int64_t now, std::int64_t stamp_ns)
    {
        m_latency_ns.push_back(now - stamp_ns);
        if(m_last_ns != 0)
            m_max_gap_ns = std::max(m_max_gap_ns, now - m_last_ns);
        m_last_ns = now;
    }
};

}

#endif
