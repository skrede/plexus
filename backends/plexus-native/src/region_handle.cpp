#include "plexus/native/region_handle.h"
#include "plexus/native/detail/posix_slot_lock.h"

#include <utility>

#include <unistd.h>
#include <sys/mman.h>

namespace plexus::native {

region_handle::region_handle() noexcept
        : m_fd(-1)
        , m_base(nullptr)
        , m_owns_name(false)
        , m_name()
        , m_length(0)
        , m_sidecar_fd(-1)
{
}

region_handle::region_handle(int fd, void *base, std::size_t length, std::string name, bool owns_name) noexcept
        : m_fd(fd)
        , m_base(base)
        , m_owns_name(owns_name)
        , m_name(std::move(name))
        , m_length(length)
        , m_sidecar_fd(-1)
{
}

region_handle::region_handle(region_handle &&other) noexcept
        : m_fd(std::exchange(other.m_fd, -1))
        , m_base(std::exchange(other.m_base, nullptr))
        , m_owns_name(std::exchange(other.m_owns_name, false))
        , m_name(std::move(other.m_name))
        , m_length(std::exchange(other.m_length, std::size_t{0}))
        , m_sidecar_fd(std::exchange(other.m_sidecar_fd, -1))
{
}

region_handle &region_handle::operator=(region_handle &&other) noexcept
{
    if(this == &other)
        return *this;

    reclaim();

    m_fd         = std::exchange(other.m_fd, -1);
    m_base       = std::exchange(other.m_base, nullptr);
    m_owns_name  = std::exchange(other.m_owns_name, false);
    m_name       = std::move(other.m_name);
    m_length     = std::exchange(other.m_length, std::size_t{0});
    m_sidecar_fd = std::exchange(other.m_sidecar_fd, -1);
    return *this;
}

region_handle::~region_handle()
{
    reclaim();
}

void region_handle::reclaim() noexcept
{
    if(m_base != nullptr)
        ::munmap(m_base, m_length);
    if(m_fd >= 0)
        ::close(m_fd);
    if(m_sidecar_fd >= 0)
        ::close(m_sidecar_fd);
    if(m_owns_name)
        ::shm_unlink(m_name.c_str());

    m_base       = nullptr;
    m_fd         = -1;
    m_sidecar_fd = -1;
    m_length     = 0;
    m_owns_name  = false;
}

std::span<std::byte> region_handle::bytes() const
{
    return {static_cast<std::byte *>(m_base), m_length};
}

std::size_t region_handle::size() const noexcept
{
    return m_length;
}

plexus::shm::slot_hold region_handle::hold_slot(std::uint32_t slot) noexcept
{
    if(m_fd < 0)
        return plexus::shm::slot_hold::failed;
    return detail::hold_slot_lock(m_fd, m_sidecar_fd, m_name, slot);
}

void region_handle::release_slot(std::uint32_t slot) noexcept
{
    if(m_fd >= 0)
        detail::release_slot_lock(m_fd, m_sidecar_fd, slot);
}

bool region_handle::slot_held_elsewhere(std::uint32_t slot) const noexcept
{
    return m_fd < 0 || detail::slot_locked_elsewhere(m_fd, m_sidecar_fd, m_name, slot);
}

}
