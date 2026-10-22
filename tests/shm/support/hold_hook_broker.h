#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_HOOK_BROKER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_HOOK_BROKER_H

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::testing {

using hold_hook = plexus::detail::move_only_function<void()>;

// A handle of the wrapped broker that runs the broker's pending hook, once, just before its next slot hold or hold of
// every slot, the way a peer acting between a joiner's attach and its hold would.
template<typename Inner>
class hooked_handle
{
public:
    hooked_handle() noexcept
            : m_inner()
            , m_hook()
    {
    }

    hooked_handle(Inner inner, std::shared_ptr<hold_hook> hook) noexcept
            : m_inner(std::move(inner))
            , m_hook(std::move(hook))
    {
    }

    std::span<std::byte> bytes() const
    {
        return m_inner.bytes();
    }

    shm::slot_hold hold_slot(std::uint32_t slot)
    {
        if(m_hook && *m_hook)
            std::exchange(*m_hook, nullptr)();
        return m_inner.hold_slot(slot);
    }

    void release_slot(std::uint32_t slot) noexcept
    {
        m_inner.release_slot(slot);
    }

    bool slot_held_elsewhere(std::uint32_t slot) const
    {
        return m_inner.slot_held_elsewhere(slot);
    }

    shm::slot_hold hold_all_slots()
    {
        if(m_hook && *m_hook)
            std::exchange(*m_hook, nullptr)();
        return m_inner.hold_all_slots();
    }

    void release_all_slots() noexcept
    {
        m_inner.release_all_slots();
    }

    void unlink_name() noexcept
    {
        m_inner.unlink_name();
    }

private:
    Inner m_inner;
    std::shared_ptr<hold_hook> m_hook;
};

template<typename Inner>
class hold_hook_broker
{
public:
    using region_handle = hooked_handle<typename Inner::region_handle>;

    hold_hook_broker()
            : m_inner()
            , m_hook(std::make_shared<hold_hook>())
    {
    }

    shm::region_status create(std::string_view name, std::size_t bytes, const shm::create_options &opts, region_handle &out)
    {
        typename Inner::region_handle inner;
        const shm::region_status status = m_inner.create(name, bytes, opts, inner);
        out                             = region_handle{std::move(inner), m_hook};
        return status;
    }

    shm::region_status attach(std::string_view name, region_handle &out)
    {
        typename Inner::region_handle inner;
        const shm::region_status status = m_inner.attach(name, inner);
        out                             = region_handle{std::move(inner), m_hook};
        return status;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
    {
        m_inner.set_attach_policy(std::move(policy));
    }

    void before_next_hold(hold_hook hook)
    {
        *m_hook = std::move(hook);
    }

private:
    Inner m_inner;
    std::shared_ptr<hold_hook> m_hook;
};

}

#endif
