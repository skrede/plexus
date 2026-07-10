#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_FLAT_MAP_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_FLAT_MAP_H

#include "plexus/detail/fail_closed.h"

#include <array>
#include <cstddef>

namespace plexus::io::detail {

// A dep-free, fixed-capacity flat map: a linear scan over N slots keyed by an
// equality-comparable Key. The (N+1)-th DISTINCT key calls plexus::detail::fail_closed
// — a DEFINED refusal, never an out-of-bounds write, a silent drop, or an undefined
// terminate. Mirrors fixed_peer_storage's occupied-flag slot array.
template<typename Key, typename Value, std::size_t N>
class fixed_flat_map
{
public:
    Value &at_or_insert(const Key &key)
    {
        if(slot *s = locate(key))
            return s->value;
        for(slot &s : m_slots)
        {
            if(!s.occupied)
            {
                s.occupied = true;
                s.key      = key;
                s.value    = Value{};
                return s.value;
            }
        }
        plexus::detail::fail_closed("fixed_flat_map: capacity exceeded");
    }

    Value *find(const Key &key)
    {
        if(slot *s = locate(key))
            return &s->value;
        return nullptr;
    }

    const Value *find(const Key &key) const
    {
        if(const slot *s = locate(key))
            return &s->value;
        return nullptr;
    }

    void erase(const Key &key)
    {
        if(slot *s = locate(key))
            s->occupied = false;
    }

    template<typename Fn>
    void for_each(Fn fn)
    {
        for(slot &s : m_slots)
            if(s.occupied)
                fn(s.key, s.value);
    }

    template<typename Pred>
    void erase_if(Pred pred)
    {
        for(slot &s : m_slots)
            if(s.occupied && pred(s.key))
                s.occupied = false;
    }

private:
    struct slot
    {
        Key key{};
        Value value{};
        bool occupied{false};
    };

    slot *locate(const Key &key)
    {
        for(slot &s : m_slots)
            if(s.occupied && s.key == key)
                return &s;
        return nullptr;
    }
    const slot *locate(const Key &key) const
    {
        for(const slot &s : m_slots)
            if(s.occupied && s.key == key)
                return &s;
        return nullptr;
    }

    std::array<slot, N> m_slots{};
};

}

#endif
