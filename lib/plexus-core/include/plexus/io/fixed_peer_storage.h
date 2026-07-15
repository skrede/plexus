#ifndef HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H

#include "plexus/node_id.h"

#include "plexus/detail/fail_closed.h"

#include "plexus/io/endpoint.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The constrained-target (MCU) awareness backend: a dep-free, fixed-capacity flat array, a
// linear scan over N. The (N+1)-th DISTINCT identity calls plexus::detail::fail_closed — a
// DEFINED refusal, never an out-of-bounds write, a silent drop, or an undefined terminate.
template<std::size_t N>
class fixed_peer_storage
{
public:
    void put(const node_id &id, const endpoint &ep)
    {
        put(id, ep, 0);
    }

    void put(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        if(entry *slot = find(id))
        {
            slot->ep             = ep;
            slot->last_refreshed = now;
            return;
        }
        for(entry &e : m_slots)
        {
            if(!e.occupied)
            {
                e.occupied       = true;
                e.id             = id;
                e.ep             = ep;
                e.last_refreshed = now;
                return;
            }
        }
        plexus::detail::fail_closed("fixed_peer_storage: capacity exceeded");
    }

    void refresh(const node_id &id, std::uint64_t now)
    {
        if(entry *slot = find(id))
            slot->last_refreshed = now;
    }

    std::optional<endpoint> get(const node_id &id) const
    {
        if(const entry *slot = find(id))
            return slot->ep;
        return std::nullopt;
    }

    bool has(const node_id &id) const
    {
        return find(id) != nullptr;
    }

    void remove(const node_id &id)
    {
        if(entry *slot = find(id))
            slot->occupied = false;
    }

    template<typename Report>
    void expire_older_than(std::uint64_t deadline, Report report)
    {
        for(entry &e : m_slots)
            if(e.occupied && e.last_refreshed < deadline)
            {
                report(e.id);
                e.occupied = false;
            }
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const entry &e : m_slots)
            if(e.occupied)
                fn(e.id, e.ep);
    }

private:
    struct entry
    {
        node_id id{};
        endpoint ep{};
        std::uint64_t last_refreshed{0};
        bool occupied{false};
    };

    entry *find(const node_id &id)
    {
        for(entry &e : m_slots)
            if(e.occupied && e.id == id)
                return &e;
        return nullptr;
    }
    const entry *find(const node_id &id) const
    {
        for(const entry &e : m_slots)
            if(e.occupied && e.id == id)
                return &e;
        return nullptr;
    }

    std::array<entry, N> m_slots{};
};

}

#endif
