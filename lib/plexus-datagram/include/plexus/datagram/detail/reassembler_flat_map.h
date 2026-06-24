#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_REASSEMBLER_FLAT_MAP_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_REASSEMBLER_FLAT_MAP_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::datagram::detail {

// A bounded, alloc-free-in-steady-state associative container keyed by an
// attacker-controlled std::uint16_t (the reassembly msg_id), replacing the
// red-black tree whose every find/emplace/erase was a node allocation on the
// untrusted path. Entries live in a sorted std::vector resolved by lower_bound
// binary search; the vector reaches its high-water mark and is reused, so steady
// state allocates nothing. The element count is capped at a ctor-supplied
// max_entries derived from the reassembler's total-memory cap (the count the cap
// can never let the table exceed), so the byte cap stays the binding backstop and
// the count limit never refuses an entry the cap would have admitted; an insert
// past max_entries refuses (the caller maps that to the existing dropped_cap).
template<typename Value>
class reassembler_flat_map
{
public:
    using key_type   = std::uint16_t;
    using value_type = std::pair<key_type, Value>;
    using container  = std::vector<value_type>;
    using iterator   = typename container::iterator;

    explicit reassembler_flat_map(std::size_t max_entries)
            : m_max_entries(max_entries)
    {
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_entries.size();
    }

    [[nodiscard]] iterator begin() noexcept
    {
        return m_entries.begin();
    }
    [[nodiscard]] iterator end() noexcept
    {
        return m_entries.end();
    }

    [[nodiscard]] iterator find(key_type key) noexcept
    {
        auto it = lower_bound(key);
        return (it != m_entries.end() && it->first == key) ? it : m_entries.end();
    }

    // Insert a new key (the caller guarantees it is absent via a prior find()).
    // Returns end() when the table is at its cap-implied ceiling — the caller maps
    // that to dropped_cap, leaving the byte cap as the hard memory backstop.
    iterator emplace(key_type key, Value value)
    {
        if(m_entries.size() >= m_max_entries)
            return m_entries.end();
        auto at = lower_bound(key);
        return m_entries.insert(at, value_type{key, std::move(value)});
    }

    void erase(iterator it) noexcept
    {
        m_entries.erase(it);
    }

private:
    iterator lower_bound(key_type key) noexcept
    {
        return std::lower_bound(m_entries.begin(), m_entries.end(), key, [](const value_type &e, key_type k) { return e.first < k; });
    }

    std::size_t m_max_entries;
    container   m_entries;
};

// The element ceiling the byte cap can never let the table exceed: the cheapest
// admissible open charges at least min_entry_cost, so the cap admits at most this
// many concurrent partials. Sized here (not tighter) so the byte cap stays the
// binding backstop and the count limit never refuses an open the cap would admit.
constexpr std::size_t cap_implied_max_partials(std::size_t total_memory_cap, std::size_t min_entry_cost) noexcept
{
    return total_memory_cap / min_entry_cost + 1u;
}

}

#endif
