#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_VIEWS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_VIEWS_H

#include <array>
#include <cstddef>

namespace plexus::freertos::detail {

// A bounded, no-heap set of non-owning channel VIEWS for the transport's poll-all. The engine owns
// the channels; a view is a raw pointer only because a cleared slot is a real modelled state (the
// nullable-lookup carve-out), never an owning pointer — it is cleared on channel close so poll-all
// never touches a dangling channel. Capacity N is the compile-time accept cap; full() rejects a
// connection over the bound (the set never grows). For N=1 the loops collapse to the single-slot
// case the dial-only path compiles to (zero overhead when unused).
template<typename Channel, std::size_t N>
class lwip_channel_views
{
public:
    lwip_channel_views()
            : m_views{}
            , m_count(0)
    {
    }

    bool full() const
    {
        return m_count == N;
    }

    bool add(Channel *view)
    {
        if(m_count == N)
            return false;
        m_views[m_count++] = view;
        return true;
    }

    void remove(Channel *view)
    {
        for(std::size_t i = 0; i < m_count; ++i)
            if(m_views[i] == view)
            {
                m_views[i] = m_views[--m_count];
                return;
            }
    }

    std::size_t count() const
    {
        return m_count;
    }

    // N=1 elides the multi machinery: the single-slot case collapses to the lone presence check the
    // dial-only path compiles to (no loop back-edge), so a single-client device pays nothing for the
    // bounded-set generalization. N>1 is the poll-all loop over the live views.
    template<typename Fn>
    void poll_each(Fn &&fn)
    {
        if constexpr(N == 1)
        {
            if(m_count)
                fn(*m_views[0]);
        }
        else
        {
            for(std::size_t i = 0; i < m_count; ++i)
                fn(*m_views[i]);
        }
    }

private:
    std::array<Channel *, N> m_views;
    std::size_t              m_count;
};

}

#endif
