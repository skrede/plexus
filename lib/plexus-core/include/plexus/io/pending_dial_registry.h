#ifndef HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H

#include "plexus/io/byte_channel.h"
#include "plexus/io/detail/retiring_table.h"

#include <memory>
#include <ranges>
#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::io {

// The half-open dial table and the accepted table a transport holds its channels in until the engine adopts them. A
// fail edge may fire inside the channel's own member call, so no verb here destroys a channel: fail(), fail_accepted(),
// retire_all() and a deliver_or_retire() with no consumer hand it to the retiring table, which keeps it until its own
// on_closed.
template<typename Channel>
    requires io::byte_channel<Channel>
class pending_dial_registry
{
    using table = std::unordered_map<Channel *, std::unique_ptr<Channel>>;

public:
    pending_dial_registry()
            : m_pending()
            , m_accepted()
            , m_retiring()
    {
    }

    pending_dial_registry(const pending_dial_registry &)            = delete;
    pending_dial_registry &operator=(const pending_dial_registry &) = delete;

    void insert(Channel *raw, std::unique_ptr<Channel> channel)
    {
        m_pending.insert_or_assign(raw, std::move(channel));
    }

    std::unique_ptr<Channel> resolve(Channel *raw)
    {
        return take(m_pending, raw);
    }

    bool is_pending(Channel *raw) const
    {
        return m_pending.contains(raw);
    }

    void fail(Channel *raw)
    {
        m_retiring.retire(resolve(raw));
    }

    void insert_accepted(Channel *raw, std::unique_ptr<Channel> channel)
    {
        m_accepted.insert_or_assign(raw, std::move(channel));
    }

    std::unique_ptr<Channel> adopt_accepted(Channel *raw)
    {
        return take(m_accepted, raw);
    }

    void fail_accepted(Channel *raw)
    {
        m_retiring.retire(adopt_accepted(raw));
    }

    template<typename Sink, typename... Args>
    void deliver_or_retire(Sink &sink, std::unique_ptr<Channel> channel, const Args &...args)
    {
        m_retiring.deliver_or_retire(sink, std::move(channel), args...);
    }

    void retire_all()
    {
        auto pending  = std::exchange(m_pending, {});
        auto accepted = std::exchange(m_accepted, {});
        for(auto &channel : std::views::values(pending))
            m_retiring.retire(std::move(channel));
        for(auto &channel : std::views::values(accepted))
            m_retiring.retire(std::move(channel));
    }

    std::size_t pending_size() const noexcept
    {
        return m_pending.size();
    }

    std::size_t accepted_size() const noexcept
    {
        return m_accepted.size();
    }

    std::size_t retiring_size() const noexcept
    {
        return m_retiring.size();
    }

private:
    table m_pending;
    table m_accepted;
    detail::retiring_table<Channel> m_retiring;

    static std::unique_ptr<Channel> take(table &from, Channel *raw)
    {
        auto it = from.find(raw);
        if(it == from.end())
            return nullptr;
        auto channel = std::move(it->second);
        from.erase(it);
        return channel;
    }
};

}

#endif
