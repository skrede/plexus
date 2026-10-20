#include "plexus/native/win_region_handle.h"
#include "plexus/native/detail/win_slot_mutex.h"

#include <array>
#include <string>
#include <cstdint>
#include <utility>

#include <windows.h>

namespace plexus::native {

win_region_handle::win_region_handle() noexcept
        : m_base(nullptr)
        , m_mapping(nullptr)
        , m_name()
        , m_length(0)
        , m_slots{}
{
}

win_region_handle::win_region_handle(void *mapping, void *base, std::size_t length, std::string name) noexcept
        : m_base(base)
        , m_mapping(mapping)
        , m_name(std::move(name))
        , m_length(length)
        , m_slots{}
{
}

win_region_handle::win_region_handle(win_region_handle &&other) noexcept
        : m_base(std::exchange(other.m_base, nullptr))
        , m_mapping(std::exchange(other.m_mapping, nullptr))
        , m_name(std::move(other.m_name))
        , m_length(std::exchange(other.m_length, std::size_t{0}))
        , m_slots(std::exchange(other.m_slots, {}))
{
}

win_region_handle &win_region_handle::operator=(win_region_handle &&other) noexcept
{
    if(this == &other)
        return *this;

    reclaim();

    m_base    = std::exchange(other.m_base, nullptr);
    m_mapping = std::exchange(other.m_mapping, nullptr);
    m_name    = std::move(other.m_name);
    m_length  = std::exchange(other.m_length, std::size_t{0});
    m_slots   = std::exchange(other.m_slots, {});
    return *this;
}

win_region_handle::~win_region_handle()
{
    reclaim();
}

void win_region_handle::reclaim() noexcept
{
    for(void *&slot : m_slots)
        detail::close_slot_mutex(slot);
    if(m_base != nullptr)
        ::UnmapViewOfFile(m_base);
    if(m_mapping != nullptr)
        ::CloseHandle(static_cast<HANDLE>(m_mapping));

    m_mapping = nullptr;
    m_base    = nullptr;
    m_length  = 0;
}

std::span<std::byte> win_region_handle::bytes() const
{
    return {static_cast<std::byte *>(m_base), m_length};
}

std::size_t win_region_handle::size() const noexcept
{
    return m_length;
}

plexus::shm::slot_hold win_region_handle::hold_slot(std::uint32_t slot)
{
    if(slot >= m_slots.size() || m_name.empty())
        return plexus::shm::slot_hold::failed;
    if(m_slots[slot] != nullptr)
        return plexus::shm::slot_hold::held;
    return detail::create_slot_mutex(m_name, slot, m_slots[slot]);
}

void win_region_handle::release_slot(std::uint32_t slot) noexcept
{
    if(slot < m_slots.size())
        detail::close_slot_mutex(m_slots[slot]);
}

// A slot this handle holds cannot be held by anyone else: a second create of the name reports it taken.
bool win_region_handle::slot_held_elsewhere(std::uint32_t slot) const
{
    if(slot >= m_slots.size() || m_name.empty())
        return true;
    return m_slots[slot] == nullptr && detail::slot_mutex_exists(m_name, slot);
}

plexus::shm::slot_hold win_region_handle::hold_all_slots()
{
    if(m_name.empty())
        return plexus::shm::slot_hold::failed;
    std::array<bool, plexus::shm::k_liveness_slots> created{};
    for(std::uint32_t slot = 0; slot < m_slots.size(); ++slot)
    {
        if(m_slots[slot] != nullptr)
            continue;
        if(const plexus::shm::slot_hold hold = detail::create_slot_mutex(m_name, slot, m_slots[slot]); hold != plexus::shm::slot_hold::held)
        {
            for(std::uint32_t undo = 0; undo < slot; ++undo)
                if(created[undo])
                    detail::close_slot_mutex(m_slots[undo]);
            return hold;
        }
        created[slot] = true;
    }
    return plexus::shm::slot_hold::held;
}

void win_region_handle::release_all_slots() noexcept
{
    for(void *&slot : m_slots)
        detail::close_slot_mutex(slot);
}

void win_region_handle::unlink_name() noexcept
{
}

}
