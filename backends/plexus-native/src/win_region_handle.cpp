#include "plexus/native/win_region_handle.h"

#include <utility>

#include <windows.h>

namespace plexus::native {

win_region_handle::win_region_handle(void *mapping, void *base, std::size_t length) noexcept
        : m_mapping(mapping)
        , m_base(base)
        , m_length(length)
{
}

win_region_handle::win_region_handle(win_region_handle &&other) noexcept
        : m_mapping(other.m_mapping)
        , m_base(other.m_base)
        , m_length(other.m_length)
{
    other.m_mapping = nullptr;
    other.m_base    = nullptr;
    other.m_length  = 0;
}

win_region_handle &win_region_handle::operator=(win_region_handle &&other) noexcept
{
    if(this == &other)
        return *this;

    reclaim();

    m_mapping = other.m_mapping;
    m_base    = other.m_base;
    m_length  = other.m_length;

    other.m_mapping = nullptr;
    other.m_base    = nullptr;
    other.m_length  = 0;
    return *this;
}

win_region_handle::~win_region_handle()
{
    reclaim();
}

void win_region_handle::reclaim() noexcept
{
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

}
