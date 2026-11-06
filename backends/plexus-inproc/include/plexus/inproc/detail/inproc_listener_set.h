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

template<typename Clock>
class inproc_listener_set
{
public:
    using accept_fn = move_only_function<void(std::unique_ptr<inproc::inproc_channel<Clock>>)>;

    struct entry
    {
        io::endpoint ep;
        accept_fn on_accepted_cb;
        std::uint64_t id{0};
    };

    inproc_listener_set()
            : m_entries()
            , m_next_id(1)
    {
    }

    // Ids are never reused, so a stale id removes nothing; 0 means the endpoint was already registered.
    std::uint64_t add(const io::endpoint &ep, accept_fn on_accepted)
    {
        if(find(ep))
            return 0;
        const std::uint64_t id = m_next_id++;
        m_entries.push_back(entry{ep, std::move(on_accepted), id});
        return id;
    }

    entry *find(const io::endpoint &ep) noexcept
    {
        auto it = std::ranges::find(m_entries, ep, &entry::ep);
        return it == m_entries.end() ? nullptr : std::addressof(*it);
    }

    void remove(std::uint64_t id) noexcept
    {
        std::erase_if(m_entries, [id](const entry &e) { return e.id == id; });
    }

private:
    std::vector<entry> m_entries;
    std::uint64_t m_next_id;
};

}

#endif
