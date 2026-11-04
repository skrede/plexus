#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_KEYED_SET_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_KEYED_SET_H

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace plexus::io::detail {

class keyed_set
{
public:
    bool insert(std::string_view k1, std::string_view k2)
    {
        return m_members[std::string{k1}].emplace(k2).second;
    }

    bool erase(std::string_view k1, std::string_view k2)
    {
        auto outer_it = m_members.find(std::string{k1});
        if(outer_it == m_members.end() || outer_it->second.erase(std::string{k2}) == 0)
            return false;
        if(outer_it->second.empty())
            m_members.erase(outer_it);
        return true;
    }

    bool holds(std::string_view k1, std::string_view k2) const
    {
        auto outer_it = m_members.find(std::string{k1});
        return outer_it != m_members.end() && outer_it->second.contains(std::string{k2});
    }

    void forget(std::string_view k1)
    {
        m_members.erase(std::string{k1});
    }

private:
    std::unordered_map<std::string, std::unordered_set<std::string>> m_members;
};

}

#endif
