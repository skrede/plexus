#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_LISTENER_SET_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_LISTENER_SET_H

#include "plexus/io/endpoint.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::inproc {

template<typename Clock>
class inproc_channel;

}

// In plexus::detail beside dispatch_packet, for the same reason: a plexus::inproc::detail would shadow the bus's bare
// detail::move_only_function lookups.
namespace plexus::detail {

// Entries are heap nodes, so a callback that adds a listener relocates none; a removal while any callback runs only
// marks its entry, which the outermost dispatch reaps once it unwinds.
template<typename Clock>
class inproc_listener_set
{
    struct dispatch_guard
    {
        inproc_listener_set &owner;
        explicit dispatch_guard(inproc_listener_set &set) noexcept
                : owner(set)
        {
            ++owner.m_dispatch_depth;
        }
        dispatch_guard(const dispatch_guard &)            = delete;
        dispatch_guard &operator=(const dispatch_guard &) = delete;
        ~dispatch_guard()
        {
            if(--owner.m_dispatch_depth == 0)
                owner.reap();
        }
    };

public:
    using accept_fn = move_only_function<void(std::unique_ptr<inproc::inproc_channel<Clock>>)>;

    struct entry
    {
        io::endpoint ep;
        accept_fn on_accepted_cb;
        std::uint64_t id{0};
        bool removed{false};
    };

    inproc_listener_set()
            : m_entries()
            , m_dispatch_depth(0)
            , m_next_id(1)
    {
    }

    // Ids are never reused, so a stale id removes nothing; 0 means the endpoint was already registered.
    std::uint64_t add(const io::endpoint &ep, accept_fn on_accepted)
    {
        if(find(ep))
            return 0;
        const std::uint64_t id = m_next_id++;
        m_entries.push_back(std::make_unique<entry>(entry{ep, std::move(on_accepted), id}));
        return id;
    }

    template<typename Deliver>
    bool dispatch(const io::endpoint &ep, Deliver deliver)
    {
        entry *e = find(ep);
        if(!e)
            return false;
        dispatch_guard guard(*this);
        deliver(e->on_accepted_cb);
        return true;
    }

    void remove(std::uint64_t id) noexcept
    {
        for(const std::unique_ptr<entry> &e : m_entries)
            if(e->id == id)
                e->removed = true;
        if(m_dispatch_depth == 0)
            reap();
    }

private:
    std::vector<std::unique_ptr<entry>> m_entries;
    std::uint32_t m_dispatch_depth;
    std::uint64_t m_next_id;

    entry *find(const io::endpoint &ep) noexcept
    {
        auto it = std::ranges::find_if(m_entries, [&ep](const std::unique_ptr<entry> &e) { return !e->removed && e->ep == ep; });
        return it == m_entries.end() ? nullptr : it->get();
    }

    void reap() noexcept
    {
        std::erase_if(m_entries, [](const std::unique_ptr<entry> &e) { return e->removed; });
    }
};

}

#endif
