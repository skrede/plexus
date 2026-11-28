#include "plexus/native/detail/win_shm_status.h"

#include <string>
#include <utility>

namespace plexus::native::detail {

namespace {

constexpr std::string::size_type k_name_max = 255;

// Per-session object namespace (never Global\, which needs SeCreateGlobalPrivilege);
// the broker owns the prefix, the caller passes a bare name (the bare-hex token).
constexpr const char *k_canonical_prefix = "Local\\plexus.";

}

// Reject empty names and names carrying a namespace separator (the sanitized name is the caller portion only; the
// broker prepends the prefix, which owns its own backslash), then length-bound the composed name.
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
        case ERROR_OUTOFMEMORY:
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
            return rs::no_space;
        case shm_error::name_invalid:
        case shm_error::map_failed:
        case shm_error::unknown:
            return rs::failed;
    }
    return rs::failed;
}

}
