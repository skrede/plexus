#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/shm_error.h"

#include <utility>

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace plexus::native {

namespace {

// POSIX NAME_MAX on Linux /dev/shm is 255 chars (the name after the leading
// slash). A compile-time constant rather than a system header pull; the bound is
// stable across the supported targets.
constexpr std::string::size_type k_name_max = 255;

// Namespacing prefix prepended to every caller name (after the leading slash).
// Avoids accidental collisions with other applications in the single global
// /dev/shm namespace. The broker owns the slash + prefix; the caller passes a
// bare name (e.g. the bare-hex region name), never a slash-prefixed path.
constexpr const char *k_canonical_prefix = "/plexus.";

// Validate a caller-supplied bare name and normalize it to the canonical POSIX
// object name the backend opens. Rejects empty names, names with an embedded
// slash, and names whose canonical form exceeds NAME_MAX -> name_invalid.
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

// Open the region O_CREAT|O_EXCL, reclaiming a stale name once when the caller opted in (the
// single-owner ring reclaims a crashed creator's orphan). Returns the fd, or -1 with errno set.
int open_create_excl(const char *canonical, mode_t perms, bool unlink_stale)
{
    int fd = ::shm_open(canonical, O_CREAT | O_EXCL | O_RDWR, perms);
    if(fd < 0 && errno == EEXIST && unlink_stale)
    {
        ::shm_unlink(canonical);
        fd = ::shm_open(canonical, O_CREAT | O_EXCL | O_RDWR, perms);
    }
    return fd;
}

shm_error map_errno(int e)
{
    switch(e)
    {
        case EEXIST:       return shm_error::already_exists;
        case ENOENT:       return shm_error::not_found;
        case EACCES:
        case EPERM:        return shm_error::permission_denied;
        case ENOSPC:       return shm_error::no_space;
        case EINVAL:
        case ENAMETOOLONG: return shm_error::name_invalid;
        default:           return shm_error::unknown;
    }
}

std::size_t page_round_up(std::size_t n)
{
    const auto page = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    return ((n + page - 1) / page) * page;
}

// Fold the backend-internal errno mapping onto the core status the concept
// returns, so core never observes an errno or shm_error.
plexus::shm::region_status to_status(shm_error e)
{
    using rs = plexus::shm::region_status;
    switch(e)
    {
        case shm_error::ok:                return rs::ok;
        case shm_error::already_exists:    return rs::already_exists;
        case shm_error::not_found:         return rs::not_found;
        case shm_error::permission_denied: return rs::denied;
        case shm_error::no_space:          return rs::failed;
        case shm_error::name_invalid:      return rs::failed;
        case shm_error::map_failed:        return rs::failed;
        case shm_error::unknown:           return rs::failed;
    }
    return rs::failed;
}

}

plexus::shm::region_status posix_shm_region_broker::finish_create(int fd, std::size_t length, std::string canonical, region_handle &out)
{
    if(::ftruncate(fd, static_cast<off_t>(length)) != 0)
    {
        const int e = errno;
        ::close(fd);
        ::shm_unlink(canonical.c_str());
        return to_status(map_errno(e));
    }
    void *base = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        ::close(fd);
        ::shm_unlink(canonical.c_str());
        return to_status(shm_error::map_failed);
    }
    out = region_handle{fd, base, length, std::move(canonical), true};
    return plexus::shm::region_status::ok;
}

plexus::shm::region_status posix_shm_region_broker::finish_attach(int fd, std::string canonical, region_handle &out)
{
    struct stat st{};
    if(::fstat(fd, &st) != 0)
    {
        const int e = errno;
        ::close(fd);
        return to_status(map_errno(e));
    }
    const auto length = static_cast<std::size_t>(st.st_size);
    void      *base   = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        ::close(fd);
        return to_status(shm_error::map_failed);
    }
    out = region_handle{fd, base, length, std::move(canonical), false};
    return plexus::shm::region_status::ok;
}

plexus::shm::region_status posix_shm_region_broker::create(std::string_view name, std::size_t bytes, const plexus::shm::create_options &opts, region_handle &out)
{
    out = region_handle{};

    // bytes==0 is caller misuse, not a recoverable map fault. The named ceiling
    // is the real DoS bound: fast-fail BEFORE any syscall, so the subsequent page
    // round-up cannot overflow.
    if(bytes == 0)
        return plexus::shm::region_status::failed;
    if(bytes > k_max_region_size)
        return plexus::shm::region_status::too_large;

    std::string canonical;
    if(const auto status = sanitize_region_name(name, canonical); status != shm_error::ok)
        return to_status(status);

    const int fd = open_create_excl(canonical.c_str(), static_cast<mode_t>(opts.perms), opts.unlink_stale_on_create);
    if(fd < 0)
        return to_status(map_errno(errno));
    return finish_create(fd, page_round_up(bytes), std::move(canonical), out);
}

plexus::shm::region_status posix_shm_region_broker::attach(std::string_view name, region_handle &out)
{
    out = region_handle{};

    std::string canonical;
    if(const auto status = sanitize_region_name(name, canonical); status != shm_error::ok)
        return to_status(status);

    if(m_attach_policy && !m_attach_policy(canonical))
        return plexus::shm::region_status::denied;

    const int fd = ::shm_open(canonical.c_str(), O_RDWR, 0);
    if(fd < 0)
        return to_status(map_errno(errno));
    return finish_attach(fd, std::move(canonical), out);
}

void posix_shm_region_broker::set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    m_attach_policy = std::move(policy);
}

}
