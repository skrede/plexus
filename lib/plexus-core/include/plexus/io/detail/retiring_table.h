#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_RETIRING_TABLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_RETIRING_TABLE_H

#include "plexus/io/byte_channel.h"

#include <memory>
#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::io::detail {

// An unclaimed channel leaves only through its own on_closed: retire() parks it, closes it, and erases it by identity
// inside its own posted notification, so nothing here destroys a channel with an operation outstanding.
template<typename Channel>
    requires io::byte_channel<Channel>
class retiring_table
{
public:
    retiring_table()
            : m_retiring()
    {
    }

    retiring_table(const retiring_table &)            = delete;
    retiring_table &operator=(const retiring_table &) = delete;

    void retire(std::unique_ptr<Channel> channel)
    {
        if(!channel)
            return;
        Channel &retired = park(std::move(channel));
        retired.on_closed([this, &retired] { (void)m_retiring.extract(std::addressof(retired)); });
        retired.close();
    }

    // then runs inside the channel's own close notification, after the channel is destroyed.
    template<typename Then>
    void retire(std::unique_ptr<Channel> channel, Then then)
    {
        if(!channel)
            return;
        Channel &retired = park(std::move(channel));
        retired.on_closed(
                [this, &retired, then = std::move(then)]() mutable
                {
                    (void)m_retiring.extract(std::addressof(retired));
                    then();
                });
        retired.close();
    }

    template<typename Sink, typename... Args>
    void deliver_or_retire(Sink &sink, std::unique_ptr<Channel> channel, const Args &...args)
    {
        if(!channel)
            return;
        if(sink)
            sink(std::move(channel), args...);
        else
            retire(std::move(channel));
    }

    std::size_t size() const noexcept
    {
        return m_retiring.size();
    }

private:
    std::unordered_map<Channel *, std::unique_ptr<Channel>> m_retiring;

    Channel &park(std::unique_ptr<Channel> channel)
    {
        Channel &retired = *channel;
        m_retiring.insert_or_assign(std::addressof(retired), std::move(channel));
        return retired;
    }
};

}

#endif
