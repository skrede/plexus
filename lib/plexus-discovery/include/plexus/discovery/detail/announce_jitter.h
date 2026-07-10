#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_ANNOUNCE_JITTER_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_ANNOUNCE_JITTER_H

#include "plexus/node_id.h"

#include <random>
#include <cstdint>

namespace plexus::discovery::detail {

// Fold the 16-byte node id into a seed so each node's announce sequence is decorrelated from its
// peers yet reproducible from the id alone (no global RNG, no clock read).
inline std::uint32_t jitter_seed_from(const node_id &id)
{
    std::uint32_t seed = 0x811c9dc5u;
    for(const std::byte b : id)
        seed = (seed ^ static_cast<std::uint32_t>(std::to_integer<unsigned char>(b))) * 0x01000193u;
    return seed;
}

// Decorrelated announce interval: next = period - uniform(0, fraction * period), clamped positive.
// A member-owned mt19937 keeps the draw allocation-free; fraction is an interim value pending an
// empirical sweep, not a tuned constant.
template<typename Duration>
class announce_jitter
{
public:
    explicit announce_jitter(double fraction)
            : m_engine(0)
            , m_fraction(fraction)
    {
    }

    void seed(std::uint32_t value)
    {
        m_engine.seed(value);
    }

    Duration next(Duration period)
    {
        const auto count = period.count();
        const auto span  = static_cast<std::int64_t>(m_fraction * static_cast<double>(count));
        if(span <= 0)
            return period;
        std::uniform_int_distribution<std::int64_t> dist(0, span);
        const auto reduced = count - dist(m_engine);
        return Duration{reduced > 0 ? reduced : 1};
    }

private:
    std::mt19937 m_engine;
    double m_fraction;
};

}

#endif
