#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_DEMAND_TABLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_DEMAND_TABLE_H

#include <string>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <unordered_map>

namespace plexus::io::detail {

// A sliding-window duplicate filter (the anti-replay bitmap of RFC 4303 section 3.4.3) that stays open until armed. Once
// armed it refuses only a sequence it has already admitted: an unseen sequence, older or newer, is admitted, and one
// older than the window is admitted because its history is unknown.
class sequence_gate
{
public:
    sequence_gate()
            : m_armed(false)
            , m_seen(0)
            , m_high_water(0)
    {
    }

    void arm(std::uint64_t first_sequence) noexcept
    {
        if(m_armed)
            return;
        m_armed      = true;
        m_seen       = 0;
        m_high_water = first_sequence;
    }

    bool admit(std::uint64_t sequence) noexcept
    {
        if(!m_armed)
            return true;
        if(sequence > m_high_water)
        {
            const std::uint64_t shift = sequence - m_high_water;
            m_seen                    = (shift < window ? m_seen << shift : 0) | std::uint64_t{1};
            m_high_water              = sequence;
            return true;
        }
        const std::uint64_t age = m_high_water - sequence;
        if(age >= window)
            return true;
        const std::uint64_t bit = std::uint64_t{1} << age;
        const bool unseen       = (m_seen & bit) == 0;
        m_seen |= bit;
        return unseen;
    }

private:
    static constexpr std::uint64_t window = 64;

    bool m_armed;
    std::uint64_t m_seen;
    std::uint64_t m_high_water;
};

// Demand keyed by (peer node name, topic fqn), each entry carrying its sequence gate. Lookups take string views and
// build no key string.
class demand_table
{
    struct key_hash
    {
        using is_transparent = void;

        std::size_t operator()(std::string_view key) const noexcept
        {
            return std::hash<std::string_view>{}(key);
        }
    };

    template<typename Value>
    using keyed = std::unordered_map<std::string, Value, key_hash, std::equal_to<>>;

public:
    bool insert(std::string_view k1, std::string_view k2)
    {
        auto outer_it = m_members.find(k1);
        if(outer_it == m_members.end())
            outer_it = m_members.try_emplace(std::string{k1}).first;
        return outer_it->second.try_emplace(std::string{k2}).second;
    }

    bool erase(std::string_view k1, std::string_view k2)
    {
        auto outer_it = m_members.find(k1);
        if(outer_it == m_members.end())
            return false;
        auto inner_it = outer_it->second.find(k2);
        if(inner_it == outer_it->second.end())
            return false;
        outer_it->second.erase(inner_it);
        if(outer_it->second.empty())
            m_members.erase(outer_it);
        return true;
    }

    sequence_gate *gate_for(std::string_view k1, std::string_view k2)
    {
        auto outer_it = m_members.find(k1);
        if(outer_it == m_members.end())
            return nullptr;
        auto inner_it = outer_it->second.find(k2);
        return inner_it == outer_it->second.end() ? nullptr : &inner_it->second;
    }

    void forget(std::string_view k1)
    {
        if(auto outer_it = m_members.find(k1); outer_it != m_members.end())
            m_members.erase(outer_it);
    }

private:
    keyed<keyed<sequence_gate>> m_members;
};

}

#endif
