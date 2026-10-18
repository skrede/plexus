#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/shm_error.h"

#include "plexus/native/detail/posix_shm_status.h"

#include <utility>

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace plexus::native {

namespace {

// Open the region O_CREAT|O_EXCL, reclaiming a stale name once when the caller opted in (the
// single-owner ring reclaims a crashed creator's orphan). Returns the fd, or -1 with errno set.
int open_create_excl(const char *canonical, mode_t perms, bool unlink_stale)
{
    int fd = ::shm_open(canonical, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, perms);
    if(fd < 0 && errno == EEXIST && unlink_stale)
    {
        ::shm_unlink(canonical);
        fd = ::shm_open(canonical, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, perms);
    }
    return fd;
}

// ftruncate on tmpfs only sets a sparse size; reserving the pages makes an exhausted /dev/shm fail here
// as no_space instead of raising SIGBUS on the first touch. macOS has no equivalent for a shm descriptor.
int reserve_pages(int fd, std::size_t length)
{
#if defined(__linux__)
    int rc = 0;
    do
        rc = ::posix_fallocate(fd, 0, static_cast<off_t>(length));
    while(rc == EINTR);
    return rc;
#else
    (void)fd;
    (void)length;
    return 0;
#endif
}

std::size_t page_round_up(std::size_t n)
{
    const auto page = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    return ((n + page - 1) / page) * page;
}

}

plexus::shm::region_status posix_shm_region_broker::finish_create(int fd, std::size_t length, std::string canonical, region_handle &out)
{
    const int e = ::ftruncate(fd, static_cast<off_t>(length)) != 0 ? errno : reserve_pages(fd, length);
    if(e != 0)
    {
        ::close(fd);
        ::shm_unlink(canonical.c_str());
        return detail::to_status(detail::map_errno(e));
    }
    void *base = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        ::close(fd);
        ::shm_unlink(canonical.c_str());
        return detail::to_status(shm_error::map_failed);
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
        return detail::to_status(detail::map_errno(e));
    }
    const auto length = static_cast<std::size_t>(st.st_size);
    void *base        = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        ::close(fd);
        return detail::to_status(shm_error::map_failed);
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
    if(const auto status = detail::sanitize_region_name(name, canonical); status != shm_error::ok)
        return detail::to_status(status);

    const int fd = open_create_excl(canonical.c_str(), static_cast<mode_t>(opts.perms), opts.unlink_stale_on_create);
    if(fd < 0)
        return detail::to_status(detail::map_errno(errno));
    return finish_create(fd, page_round_up(bytes), std::move(canonical), out);
}

plexus::shm::region_status posix_shm_region_broker::attach(std::string_view name, region_handle &out)
{
    out = region_handle{};

    std::string canonical;
    if(const auto status = detail::sanitize_region_name(name, canonical); status != shm_error::ok)
        return detail::to_status(status);

    if(m_attach_policy && !m_attach_policy(canonical))
        return plexus::shm::region_status::denied;

    const int fd = ::shm_open(canonical.c_str(), O_RDWR | O_CLOEXEC, 0);
    if(fd < 0)
        return detail::to_status(detail::map_errno(errno));
    return finish_attach(fd, std::move(canonical), out);
}

plexus::shm::region_status posix_shm_region_broker::unlink(std::string_view name)
{
    std::string canonical;
    if(const auto status = detail::sanitize_region_name(name, canonical); status != shm_error::ok)
        return detail::to_status(status);
    if(::shm_unlink(canonical.c_str()) != 0)
        return detail::to_status(detail::map_errno(errno));
    return plexus::shm::region_status::ok;
}

void posix_shm_region_broker::set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    m_attach_policy = std::move(policy);
}

}
