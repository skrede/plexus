#include "plexus/native/detail/posix_shm_status.h"

#include <string>
#include <utility>

#include <cerrno>

namespace plexus::native::detail {

namespace {

#if defined(__APPLE__)
constexpr std::string::size_type k_name_max = 30; // PSHMNAMLEN 31 minus the leading slash
#else
constexpr std::string::size_type k_name_max = 255; // POSIX NAME_MAX on Linux /dev/shm
#endif

// Namespacing prefix prepended to every caller name (after the leading slash).
// Avoids accidental collisions with other applications in the single global
// /dev/shm namespace. The broker owns the slash + prefix; the caller passes a
// bare name (e.g. the bare-hex region name), never a slash-prefixed path.
constexpr const char *k_canonical_prefix = "/plexus.";

}

shm_error sanitize_region_name(std::string_view logical, std::string &out_canonical)
{
    if(logical.empty())
        return shm_error::name_invalid;
    if(logical.find('/') != std::string_view::npos)
        return shm_error::name_invalid;

    std::string canonical = k_canonical_prefix;
    canonical.append(logical);

    // The 255-char bound applies to the name following the leading slash.
    if(canonical.size() - 1 > k_name_max)
        return shm_error::name_invalid;

    out_canonical = std::move(canonical);
    return shm_error::ok;
}

shm_error map_errno(int e)
{
    switch(e)
    {
        case EEXIST:
            return shm_error::already_exists;
        case ENOENT:
            return shm_error::not_found;
        case EACCES:
        case EPERM:
            return shm_error::permission_denied;
        case ENOSPC:
            return shm_error::no_space;
        case EINVAL:
        case ENAMETOOLONG:
            return shm_error::name_invalid;
        default:
            return shm_error::unknown;
    }
}

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
