#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_POSTED_QUEUE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_POSTED_QUEUE_H

#include "plexus/shm/shm_byte_channel.h"

#include "plexus/detail/compat.h"

#include <deque>
#include <cstddef>
#include <utility>

namespace plexus::testing {

// The executor a single-threaded shared-memory test turns by hand: posted work waits until drain() runs it in order.
class posted_queue
{
public:
    posted_queue()
            : m_work()
    {
    }

    posted_queue(const posted_queue &)            = delete;
    posted_queue &operator=(const posted_queue &) = delete;

    shm::post_fn post()
    {
        return [this](plexus::detail::move_only_function<void()> fn) { m_work.push_back(std::move(fn)); };
    }

    std::size_t size() const noexcept
    {
        return m_work.size();
    }

    void drain()
    {
        while(!m_work.empty())
        {
            auto fn = std::move(m_work.front());
            m_work.pop_front();
            fn();
        }
    }

private:
    std::deque<plexus::detail::move_only_function<void()>> m_work;
};

}

#endif
