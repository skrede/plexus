#ifndef HPP_GUARD_PLEXUS_SHM_WAITER_LOSS_H
#define HPP_GUARD_PLEXUS_SHM_WAITER_LOSS_H

#include "plexus/shm/arm_status.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <vector>
#include <cstdint>
#include <utility>

namespace plexus::shm {

// The holders watching one ring's waiter, each told once when the waiter is lost after its acquire armed it. A told
// holder may release the ring, so a report keeps this alive past the ring's entry.
class waiter_loss
{
public:
    using lost_fn = plexus::detail::move_only_function<void()>;

    waiter_loss() noexcept
            : m_watchers()
            , m_next_id(1)
            , m_status(arm_status::armed)
    {
    }

    std::uint64_t add(lost_fn lost)
    {
        m_watchers.emplace_back(m_next_id, std::move(lost));
        return m_next_id++;
    }

    void remove(std::uint64_t id)
    {
        std::erase_if(m_watchers, [id](const std::pair<std::uint64_t, lost_fn> &w) { return w.first == id; });
    }

    // armed while the waiter is intact, else why its last leg could not arm.
    arm_status status() const noexcept
    {
        return m_status;
    }

    // A watcher may destroy other holders; each one's removal keeps it from being told.
    void report(arm_status status)
    {
        m_status = status;
        while(!m_watchers.empty())
        {
            lost_fn lost = std::move(m_watchers.front().second);
            m_watchers.erase(m_watchers.begin());
            lost();
        }
    }

private:
    std::vector<std::pair<std::uint64_t, lost_fn>> m_watchers;
    std::uint64_t m_next_id;
    arm_status m_status;
};

// One holder's registration with its ring's waiter_loss, withdrawn on destruction. A null loss or an empty lost
// registers nothing.
class loss_watch
{
public:
    loss_watch(std::shared_ptr<waiter_loss> loss, waiter_loss::lost_fn lost)
            : m_loss(lost ? std::move(loss) : nullptr)
            , m_id(m_loss ? m_loss->add(std::move(lost)) : 0)
    {
    }

    ~loss_watch()
    {
        if(m_loss)
            m_loss->remove(m_id);
    }

    loss_watch(const loss_watch &)            = delete;
    loss_watch &operator=(const loss_watch &) = delete;
    loss_watch(loss_watch &&)                 = delete;
    loss_watch &operator=(loss_watch &&)      = delete;

private:
    std::shared_ptr<waiter_loss> m_loss;
    std::uint64_t m_id;
};

}

#endif
