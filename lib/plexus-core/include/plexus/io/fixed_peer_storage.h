#ifndef HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H

#include "plexus/io/endpoint.h"
#include "plexus/node_id.h"
#include "plexus/detail/fail_closed.h"

#include <array>
#include <cstddef>
#include <optional>

namespace plexus::io {

// The constrained-target awareness backend: a dep-free, fixed-capacity flat array of
// N node_id -> endpoint entries. It is the MCU-opt-in Storage policy for
// basic_known_peers<Storage> — no heap, no new dependency, no std::map. node_id has a
// defaulted operator<=> so equality is free; a linear scan over N (N is small, ~8-32) is
// the whole lookup. The four operations (put/get/has/remove) match the std::map backend
// surface exactly, so basic_known_peers delegates to either without change.
//
// FAIL-CLOSED CAPACITY: put overwrites a matching identity's slot or fills a free one. The
// (N+1)-th DISTINCT identity (no match, no free slot) calls plexus::detail::fail_closed — a
// DEFINED [[noreturn]] refusal (throw under exceptions, abort hook under
// PLEXUS_NO_EXCEPTIONS), never an out-of-bounds write, a silent drop, or an undefined
// terminate. A bounded peer table refuses to grow past its capacity.
template<std::size_t N>
class fixed_peer_storage
{
public:
    void put(const node_id &id, const endpoint &ep)
    {
        if(entry *slot = find(id))
        {
            slot->ep = ep;
            return;
        }
        for(entry &e : m_slots)
        {
            if(!e.occupied)
            {
                e.occupied = true;
                e.id       = id;
                e.ep       = ep;
                return;
            }
        }
        plexus::detail::fail_closed("fixed_peer_storage: capacity exceeded");
    }

    std::optional<endpoint> get(const node_id &id) const
    {
        if(const entry *slot = find(id))
            return slot->ep;
        return std::nullopt;
    }

    bool has(const node_id &id) const { return find(id) != nullptr; }

    void remove(const node_id &id)
    {
        if(entry *slot = find(id))
            slot->occupied = false;
    }

private:
    struct entry
    {
        node_id  id{};
        endpoint ep{};
        bool     occupied{false};
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
