#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_HINTS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_HINTS_H

#include "plexus/io/dispatch_hint.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace plexus::io::detail {

using upgrade_hint_table = std::unordered_map<std::string, dispatch_hint>;

// The coordinator's hint and policy bodies, befriended as one. The node's own publisher and subscriber hints accumulate
// per fqn; no hint rides the wire.
struct upgrade_hints
{
    template<typename Coordinator>
    static void note(Coordinator &c, std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = c.m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    template<typename Coordinator>
    static dispatch_hint hint_for(const Coordinator &c, std::string_view fqn)
    {
        auto it = c.m_hints.find(std::string{fqn});
        return it == c.m_hints.end() ? dispatch_hint::none : it->second;
    }

    // Unset, the policy is the generic default: the peer is on this host and any hint bit is set.
    template<typename Coordinator>
    static bool wants_lane(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        const bool same_host     = c.m_registry.same_host_for(node_name);
        const dispatch_hint hint = hint_for(c, fqn);
        return c.m_policy ? c.m_policy(same_host, hint) : (same_host && any_set(hint));
    }

    template<typename Coordinator>
    static void forget(Coordinator &c, std::string_view fqn)
    {
        c.m_hints.erase(std::string{fqn});
    }
};

}

#endif
