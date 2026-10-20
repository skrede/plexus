#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_CHANNEL_GRAVEYARD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_CHANNEL_GRAVEYARD_H

#include "plexus/io/byte_channel.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"

#include <array>
#include <memory>
#include <cstddef>
#include <utility>
#include <algorithm>

namespace plexus::io::detail {

// Retiring parks the channel, installs on_closed to erase it, and only then closes it, so the channel is destroyed
// inside its own notification, after its last outstanding operation. The graveyard outlives every entry it holds.
template<typename Channel, std::size_t N>
    requires io::byte_channel<Channel>
class channel_graveyard
{
    struct cell
    {
        std::unique_ptr<Channel> channel;
        plexus::detail::move_only_function<void()> on_erased_cb;

        const Channel *address() const noexcept
        {
            return channel.get();
        }
    };

public:
    channel_graveyard() noexcept
            : m_cells()
    {
    }

    channel_graveyard(const channel_graveyard &)            = delete;
    channel_graveyard &operator=(const channel_graveyard &) = delete;

    // An entry still held here at destruction is valid only once the executor has drained.
    ~channel_graveyard() = default;

    void retire(std::unique_ptr<Channel> channel, plexus::detail::move_only_function<void()> on_erased = {})
    {
        if(!channel)
            return;
        cell &parked        = free_cell();
        parked.channel      = std::move(channel);
        parked.on_erased_cb = std::move(on_erased);
        Channel &retired    = *parked.channel;
        retired.on_closed([this, &retired] { erase(retired); });
        retired.close();
    }

    bool empty() const noexcept
    {
        return size() == 0;
    }

    std::size_t size() const noexcept
    {
        return m_cells.size() - static_cast<std::size_t>(std::ranges::count(m_cells, nullptr, &cell::address));
    }

    bool holds(const Channel &channel) const noexcept
    {
        return std::ranges::find(m_cells, std::addressof(channel), &cell::address) != m_cells.end();
    }

private:
    std::array<cell, N> m_cells;

    cell &free_cell()
    {
        for(cell &c : m_cells)
            if(!c.channel)
                return c;
        plexus::detail::fail_closed("channel_graveyard: capacity exceeded");
    }

    void erase(const Channel &channel)
    {
        auto it = std::ranges::find(m_cells, std::addressof(channel), &cell::address);
        if(it == m_cells.end())
            return;
        auto on_erased = std::exchange(it->on_erased_cb, nullptr);
        it->channel.reset();
        if(on_erased)
            on_erased();
    }
};

}

#endif
