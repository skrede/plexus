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
        Channel &retired = *channel;
        m_retiring.insert_or_assign(std::addressof(retired), std::move(channel));
        retired.on_closed([this, &retired] { (void)m_retiring.extract(std::addressof(retired)); });
        retired.close();
    }

    std::size_t size() const noexcept
    {
        return m_retiring.size();
    }

private:
    std::unordered_map<Channel *, std::unique_ptr<Channel>> m_retiring;
};

}

#endif
