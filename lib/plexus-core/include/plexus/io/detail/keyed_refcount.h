#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_KEYED_REFCOUNT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_KEYED_REFCOUNT_H

#include <string>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io::detail {

// A (k1, k2) -> uint32 nested-map refcount: the single source of truth the
// per-(node_name, fqn) subscribe gate and the same-host ring-acquire gate both
// consume. bump returns the post-increment count (the 0->1 result is the caller's
// emit gate); drop returns the post-decrement count, erasing an emptied inner then
// outer entry, and returns k_no_entry for a missing pair so the caller treats it as
// "no transition".
class keyed_refcount
{
public:
    static constexpr std::uint32_t k_no_entry = ~0u;

    std::uint32_t bump(std::string_view k1, std::string_view k2)
    {
        auto &inner         = m_counts[std::string{k1}];
        auto [it, inserted] = inner.try_emplace(std::string{k2}, 0u);
        return ++it->second;
    }

    std::uint32_t drop(std::string_view k1, std::string_view k2)
    {
        auto outer_it = m_counts.find(std::string{k1});
        if(outer_it == m_counts.end())
            return k_no_entry;
        auto inner_it = outer_it->second.find(std::string{k2});
        if(inner_it == outer_it->second.end())
            return k_no_entry;
        std::uint32_t remaining = --inner_it->second;
        if(remaining == 0)
        {
            outer_it->second.erase(inner_it);
            if(outer_it->second.empty())
                m_counts.erase(outer_it);
        }
        return remaining;
    }

    [[nodiscard]] bool holds(std::string_view k1, std::string_view k2) const
    {
        auto outer_it = m_counts.find(std::string{k1});
        if(outer_it == m_counts.end())
            return false;
        auto inner_it = outer_it->second.find(std::string{k2});
        return inner_it != outer_it->second.end() && inner_it->second > 0;
    }

    void forget(std::string_view k1)
    {
        m_counts.erase(std::string{k1});
    }

private:
    std::unordered_map<std::string, std::unordered_map<std::string, std::uint32_t>> m_counts;
};

}

#endif
