#include "plexus/native/shm_error.h"
#include "plexus/native/win_shm_region_broker.h"

#include "plexus/native/detail/win_shm_status.h"
#include "plexus/native/detail/win_owner_only_security.h"

#include <windows.h>

#include <string>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::native {

namespace {

// The canonical name is pure ASCII, so a direct widening is sufficient.
std::wstring widen(const std::string &narrow)
{
    return std::wstring(narrow.begin(), narrow.end());
}

shm_error create_section(const std::string &canonical, std::size_t length, HANDLE &out_mapping)
{
    const std::wstring wname = widen(canonical);
    detail::win_owner_only_security security;
    const auto wide = static_cast<std::uint64_t>(length);
    HANDLE h        = ::CreateFileMappingW(INVALID_HANDLE_VALUE, security.get(), PAGE_READWRITE, static_cast<DWORD>(wide >> 32), static_cast<DWORD>(wide & 0xFFFFFFFFu), wname.c_str());
    if(h == nullptr)
        return detail::map_last_error(::GetLastError());
    if(::GetLastError() == ERROR_ALREADY_EXISTS)
    {
        ::CloseHandle(h);
        return shm_error::already_exists;
    }
    out_mapping = h;
    return shm_error::ok;
}

// The view length VirtualQuery reports at attach then equals the length the creator records in the ring header.
std::size_t granularity_round_up(std::size_t n)
{
    SYSTEM_INFO info{};
    ::GetSystemInfo(&info);
    const auto unit = static_cast<std::size_t>(info.dwAllocationGranularity);
    return ((n + unit - 1) / unit) * unit;
}

}

plexus::shm::region_status win_shm_region_broker::finish_create_map(void *mapping, std::size_t length, std::string canonical, region_handle &out)
{
    HANDLE h   = static_cast<HANDLE>(mapping);
    void *base = ::MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if(base == nullptr)
    {
        const DWORD e = ::GetLastError();
        ::CloseHandle(h);
        return detail::to_status(detail::map_last_error(e));
    }
    out = region_handle{mapping, base, length, std::move(canonical)};
    return plexus::shm::region_status::ok;
}

// No fstat analog: VirtualQuery recovers the view length, which equals the creator's granularity-rounded section size.
plexus::shm::region_status win_shm_region_broker::finish_attach_map(void *mapping, std::string canonical, region_handle &out)
{
    HANDLE h   = static_cast<HANDLE>(mapping);
    void *base = ::MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if(base == nullptr)
    {
        const DWORD e = ::GetLastError();
        ::CloseHandle(h);
        return detail::to_status(detail::map_last_error(e));
    }
    MEMORY_BASIC_INFORMATION mbi{};
    if(::VirtualQuery(base, &mbi, sizeof(mbi)) == 0)
    {
        const DWORD e = ::GetLastError();
        ::UnmapViewOfFile(base);
        ::CloseHandle(h);
        return detail::to_status(detail::map_last_error(e));
    }
    out = region_handle{mapping, base, static_cast<std::size_t>(mbi.RegionSize), std::move(canonical)};
    return plexus::shm::region_status::ok;
}

plexus::shm::region_status win_shm_region_broker::create(std::string_view name, std::size_t bytes, const plexus::shm::create_options &, region_handle &out)
{
    out = region_handle{};

    if(bytes == 0)
        return plexus::shm::region_status::failed;
    if(bytes > k_max_region_size)
        return plexus::shm::region_status::too_large;

    std::string canonical;
    if(const auto status = detail::sanitize_region_name(name, canonical); status != shm_error::ok)
        return detail::to_status(status);

    const std::size_t length = granularity_round_up(bytes);
    HANDLE h                 = nullptr;
    if(const auto status = create_section(canonical, length, h); status != shm_error::ok)
        return detail::to_status(status);
    return finish_create_map(h, length, std::move(canonical), out);
}

plexus::shm::region_status win_shm_region_broker::attach(std::string_view name, region_handle &out)
{
    out = region_handle{};

    std::string canonical;
    if(const auto status = detail::sanitize_region_name(name, canonical); status != shm_error::ok)
        return detail::to_status(status);

    if(m_attach_policy && !m_attach_policy(canonical))
        return plexus::shm::region_status::denied;

    const std::wstring wname = widen(canonical);
    HANDLE h                 = ::OpenFileMappingW(FILE_MAP_ALL_ACCESS, FALSE, wname.c_str());
    if(h == nullptr)
        return detail::to_status(detail::map_last_error(::GetLastError()));
    return finish_attach_map(h, std::move(canonical), out);
}

plexus::shm::region_status win_shm_region_broker::unlink(std::string_view)
{
    return plexus::shm::region_status::ok;
}

void win_shm_region_broker::set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    m_attach_policy = std::move(policy);
}

}
