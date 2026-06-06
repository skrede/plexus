#include "plexus/shm/region_handle.h"

#include <utility>

#include <unistd.h>
#include <sys/mman.h>

namespace plexus::shm {

region_handle::region_handle(int fd, void *base, std::size_t length, std::string name,
                             bool owns_name) noexcept
    : m_fd(fd), m_base(base), m_length(length), m_name(std::move(name)), m_owns_name(owns_name)
{
}

region_handle::region_handle(region_handle &&other) noexcept
    : m_fd(other.m_fd),
      m_base(other.m_base),
      m_length(other.m_length),
      m_name(std::move(other.m_name)),
      m_owns_name(other.m_owns_name)
{
    other.m_base      = nullptr;
    other.m_fd        = -1;
    other.m_length    = 0;
    other.m_owns_name = false;
}

region_handle &region_handle::operator=(region_handle &&other) noexcept
{
    if(this == &other)
        return *this;

    reclaim();

    m_fd        = other.m_fd;
    m_base      = other.m_base;
    m_length    = other.m_length;
    m_name      = std::move(other.m_name);
    m_owns_name = other.m_owns_name;

    other.m_base      = nullptr;
    other.m_fd        = -1;
    other.m_length    = 0;
    other.m_owns_name = false;
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
    if(m_owns_name)
        ::shm_unlink(m_name.c_str());

    m_base      = nullptr;
    m_fd        = -1;
    m_length    = 0;
    m_owns_name = false;
}

std::span<std::byte> region_handle::bytes() const
{
    return {static_cast<std::byte *>(m_base), m_length};
}

std::size_t region_handle::size() const noexcept
{
    return m_length;
}

}
