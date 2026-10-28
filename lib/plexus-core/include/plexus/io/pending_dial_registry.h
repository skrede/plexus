#ifndef HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/detail/retiring_table.h"

#include <memory>
#include <ranges>
#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::io {

// The dial and accepted tables a transport holds its channels in until the engine adopts them. A fail edge may fire
// inside the channel's own member call, so no verb here destroys a channel; each hands it to the retiring table. At
// close, retire_all reports each still-pending dial as operation_aborted inside that dial's own close notification,
// after erasing it, so the dial's late completions have all run first and a transport must keep them silent.
template<typename Channel>
    requires io::byte_channel<Channel>
class pending_dial_registry
{
    struct dial
    {
        std::unique_ptr<Channel> channel;
        io::endpoint target;
    };

public:
    pending_dial_registry()
            : m_pending()
            , m_accepted()
            , m_retiring()
    {
    }

    pending_dial_registry(const pending_dial_registry &)            = delete;
    pending_dial_registry &operator=(const pending_dial_registry &) = delete;

    void insert(Channel *raw, std::unique_ptr<Channel> channel, io::endpoint target)
    {
        m_pending.insert_or_assign(raw, dial{std::move(channel), std::move(target)});
    }

    std::unique_ptr<Channel> resolve(Channel *raw)
    {
        auto node = m_pending.extract(raw);
        return node ? std::move(node.mapped().channel) : nullptr;
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
        auto node = m_accepted.extract(raw);
        return node ? std::move(node.mapped()) : nullptr;
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

    template<typename Sink>
    void retire_all(Sink &on_dial_failed)
    {
        auto pending = std::exchange(m_pending, {});
        for(auto &entry : std::views::values(pending))
            m_retiring.retire(std::move(entry.channel),
                              [&on_dial_failed, target = std::move(entry.target)]
                              {
                                  if(on_dial_failed)
                                      on_dial_failed(target, io::io_error::operation_aborted);
                              });
        retire_accepted();
    }

    void retire_accepted()
    {
        auto accepted = std::exchange(m_accepted, {});
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
    std::unordered_map<Channel *, dial> m_pending;
    std::unordered_map<Channel *, std::unique_ptr<Channel>> m_accepted;
    detail::retiring_table<Channel> m_retiring;
};

}

#endif
