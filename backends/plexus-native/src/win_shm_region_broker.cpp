#include "plexus/native/win_shm_region_broker.h"
#include "plexus/native/shm_error.h"
#include "plexus/native/detail/win_owner_only_security.h"

#include <string>
#include <cstdint>
#include <utility>
#include <string_view>

#include <windows.h>

namespace plexus::native {

namespace {

constexpr std::string::size_type k_name_max = 255;

// Per-session object namespace (never Global\, which needs SeCreateGlobalPrivilege);
// the broker owns the prefix, the caller passes a bare name (the bare-hex token).
constexpr const char *k_canonical_prefix = "Local\\plexus.";

// Reject empty names and names carrying a namespace separator (the sanitized name
// is the caller portion only; the broker prepends the prefix, which owns its own
// backslash), then length-bound the composed name.
shm_error sanitize_region_name(std::string_view logical, std::string &out_canonical)
{
    if(logical.empty())
        return shm_error::name_invalid;
    if(logical.find('/') != std::string_view::npos || logical.find('\\') != std::string_view::npos)
        return shm_error::name_invalid;

    std::string canonical = k_canonical_prefix;
    canonical.append(logical);

    if(canonical.size() > k_name_max)
        return shm_error::name_invalid;

    out_canonical = std::move(canonical);
    return shm_error::ok;
}

// The canonical name is pure ASCII, so a direct widening is sufficient.
std::wstring widen(const std::string &narrow)
{
    return std::wstring(narrow.begin(), narrow.end());
}

shm_error map_last_error(DWORD e)
{
    switch(e)
    {
        case ERROR_ALREADY_EXISTS:
            return shm_error::already_exists;
        case ERROR_FILE_NOT_FOUND:
            return shm_error::not_found;
        case ERROR_ACCESS_DENIED:
            return shm_error::permission_denied;
        case ERROR_NOT_ENOUGH_MEMORY:
        case ERROR_COMMITMENT_LIMIT:
            return shm_error::no_space;
        case ERROR_INVALID_NAME:
            return shm_error::name_invalid;
        default:
            return shm_error::unknown;
    }
}

// Fold the backend-internal status onto the core status the concept returns, so
// core never observes a DWORD or shm_error.
plexus::shm::region_status to_status(shm_error e)
{
    using rs = plexus::shm::region_status;
    switch(e)
    {
        case shm_error::ok:
            return rs::ok;
        case shm_error::already_exists:
            return rs::already_exists;
        case shm_error::not_found:
            return rs::not_found;
        case shm_error::permission_denied:
            return rs::denied;
        case shm_error::no_space:
        case shm_error::name_invalid:
        case shm_error::map_failed:
        case shm_error::unknown:
            return rs::failed;
    }
    return rs::failed;
}

}

plexus::shm::region_status win_shm_region_broker::finish_create_map(void *mapping, std::size_t length, region_handle &out)
{
    HANDLE h   = static_cast<HANDLE>(mapping);
    void *base = ::MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if(base == nullptr)
    {
        const DWORD e = ::GetLastError();
        ::CloseHandle(h);
        return to_status(map_last_error(e));
    }
    out = region_handle{mapping, base, length};
    return plexus::shm::region_status::ok;
}

// No fstat analog: VirtualQuery recovers the committed view length. RegionSize
// rounds up to allocation granularity (>= the true size), which is safe because
// broadcast_ring::attach re-reads geometry from the in-region header.
plexus::shm::region_status win_shm_region_broker::finish_attach_map(void *mapping, region_handle &out)
{
    HANDLE h   = static_cast<HANDLE>(mapping);
    void *base = ::MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if(base == nullptr)
    {
        const DWORD e = ::GetLastError();
        ::CloseHandle(h);
        return to_status(map_last_error(e));
    }
    MEMORY_BASIC_INFORMATION mbi{};
    if(::VirtualQuery(base, &mbi, sizeof(mbi)) == 0)
    {
        const DWORD e = ::GetLastError();
        ::UnmapViewOfFile(base);
        ::CloseHandle(h);
        return to_status(map_last_error(e));
    }
    out = region_handle{mapping, base, static_cast<std::size_t>(mbi.RegionSize)};
    return plexus::shm::region_status::ok;
}

plexus::shm::region_status win_shm_region_broker::create(std::string_view name, std::size_t bytes, const plexus::shm::create_options &opts, region_handle &out)
{
    out = region_handle{};

    if(bytes == 0)
        return plexus::shm::region_status::failed;
    if(bytes > k_max_region_size)
        return plexus::shm::region_status::too_large;

    std::string canonical;
    if(const auto status = sanitize_region_name(name, canonical); status != shm_error::ok)
        return to_status(status);

    // Named mappings are refcounted and auto-free on last-handle-close, so there
    // is no orphan to reclaim.
    (void)opts.unlink_stale_on_create;

    const std::wstring wname = widen(canonical);
    detail::win_owner_only_security security;
    const auto wide = static_cast<std::uint64_t>(bytes);
    HANDLE h        = ::CreateFileMappingW(INVALID_HANDLE_VALUE, security.get(), PAGE_READWRITE, static_cast<DWORD>(wide >> 32), static_cast<DWORD>(wide & 0xFFFFFFFFu), wname.c_str());
    if(h == nullptr)
        return to_status(map_last_error(::GetLastError()));
    if(::GetLastError() == ERROR_ALREADY_EXISTS)
    {
        ::CloseHandle(h);
        return plexus::shm::region_status::already_exists;
    }
    return finish_create_map(h, bytes, out);
}

plexus::shm::region_status win_shm_region_broker::attach(std::string_view name, region_handle &out)
{
    out = region_handle{};

    std::string canonical;
    if(const auto status = sanitize_region_name(name, canonical); status != shm_error::ok)
        return to_status(status);

    if(m_attach_policy && !m_attach_policy(canonical))
        return plexus::shm::region_status::denied;

    const std::wstring wname = widen(canonical);
    HANDLE h                 = ::OpenFileMappingW(FILE_MAP_ALL_ACCESS, FALSE, wname.c_str());
    if(h == nullptr)
        return to_status(map_last_error(::GetLastError()));
    return finish_attach_map(h, out);
}

void win_shm_region_broker::set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    m_attach_policy = std::move(policy);
}

}
