#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_ANNOUNCEMENTS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_ANNOUNCEMENTS_H

#include "plexus/io/peer_kind.h"

#include "plexus/node_id.h"

#include <map>
#include <optional>

namespace plexus::io::detail {

// The ids an observer currently sees up, so a handover between two sessions of one id posts nothing.
class peer_announcements
{
    struct entry
    {
        peer_kind kind;
        bool ready;
    };

public:
    peer_announcements()
            : m_up()
    {
    }

    bool note_up(const node_id &id, peer_kind kind)
    {
        return m_up.try_emplace(id, entry{kind, false}).second;
    }

    bool note_ready(const node_id &id)
    {
        auto it = m_up.find(id);
        if(it == m_up.end() || it->second.ready)
            return false;
        it->second.ready = true;
        return true;
    }

    std::optional<peer_kind> note_down(const node_id &id)
    {
        auto it = m_up.find(id);
        if(it == m_up.end())
            return std::nullopt;
        const peer_kind kind = it->second.kind;
        m_up.erase(it);
        return kind;
    }

    bool is_up(const node_id &id) const
    {
        return m_up.contains(id);
    }

private:
    std::map<node_id, entry> m_up;
};

}

#endif
