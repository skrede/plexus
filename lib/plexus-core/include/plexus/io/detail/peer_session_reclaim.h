#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RECLAIM_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RECLAIM_H

#include <algorithm>

namespace plexus::io::detail {

template<typename Registry, typename Build, typename Slot>
void post_resume(Registry &reg, Build &build, Slot &slot);

template<typename Slot>
bool holds_nothing(Slot &slot);

// A queued resume holds the slot by reference, so the erase is posted only once the slot's queued resumes have
// settled, and re-posts behind any resume queued after that. The registry counts each erase from the emptied hook to
// its run, so a close holds its owner until every erase has run.
struct slot_reclaim
{
    template<typename Registry, typename Slot>
    static void arm(Registry &reg, Slot &slot)
    {
        slot.driver.close();
        slot.driver.pending().on_settled([&reg, &slot] { post_resume(reg, reg.m_build, slot); });
        slot.on_emptied_cb = [&reg, &slot] { emptied(reg, slot); };
    }

    template<typename Registry, typename Slot>
    static void emptied(Registry &reg, Slot &slot)
    {
        reg.m_reclaims.begin();
        slot.posts.close();
        slot.posts.on_settled([&reg, &slot] { post_erase(reg, slot); });
    }

    template<typename Registry, typename Slot>
    static void post_erase(Registry &reg, Slot &slot)
    {
        Slot::policy_type::post(reg.m_build.executor, [&reg, &slot] { erase(reg, slot); });
    }

    template<typename Registry, typename Slot>
    static void erase(Registry &reg, Slot &slot)
    {
        if(slot.posts.count() != 0)
            return post_erase(reg, slot);
        auto it = std::ranges::find_if(reg.m_slots, [&slot](const auto &entry) { return entry.second.get() == &slot; });
        if(it != reg.m_slots.end() && holds_nothing(slot))
            reg.m_slots.erase(it);
        reg.m_reclaims.end();
    }
};

}

#endif
