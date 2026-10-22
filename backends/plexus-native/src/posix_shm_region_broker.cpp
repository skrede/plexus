#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/shm_error.h"

#include "plexus/native/detail/posix_slot_lock.h"
#include "plexus/native/detail/posix_shm_status.h"

#include <utility>

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace plexus::native {

namespace {

// ftruncate on tmpfs only sets a sparse size, so Linux reserves the pages instead: an exhausted /dev/shm then fails here
// as no_space rather than raising SIGBUS on the first touch. shmem_fallocate (mm/shmem.c) writes the new size only after
// every page is reserved, so a failure leaves the object zero length; macOS has no reservation for a shm descriptor.
int size_object(int fd, std::size_t length)
{
#if defined(__linux__)
    int rc = 0;
    do
        rc = ::posix_fallocate(fd, 0, static_cast<off_t>(length));
    while(rc == EINTR);
    return rc;
#else
    return ::ftruncate(fd, static_cast<off_t>(length)) != 0 ? errno : 0;
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
    if(const int e = size_object(fd, length); e != 0)
    {
        ::close(fd);
        // The object is still zero length, so it carries no ring header for another process to adopt or retire, and the
        // name is the one this create made.
        ::shm_unlink(canonical.c_str());
        return detail::to_status(detail::map_errno(e));
    }
    void *base = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        // Once sized, the object may already be another producer's to retire, so its name is left to the ring protocol's
        // identity-checked removal.
        ::close(fd);
        return detail::to_status(shm_error::map_failed);
    }
    out = region_handle{fd, base, length, std::move(canonical)};
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
    // A zero-length object cannot be mapped, but a holder of the proof can still remove its name through the handle.
    void *base = length == 0 ? nullptr : ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED)
    {
        ::close(fd);
        return detail::to_status(shm_error::map_failed);
    }
    out = region_handle{fd, base, length, std::move(canonical)};
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

    const int fd = ::shm_open(canonical.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, static_cast<mode_t>(opts.perms));
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
    const int removed = ::shm_unlink(canonical.c_str());
    const int error   = errno;
    detail::unlink_sidecar(canonical);
    if(removed != 0)
        return detail::to_status(detail::map_errno(error));
    return plexus::shm::region_status::ok;
}

void posix_shm_region_broker::set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    m_attach_policy = std::move(policy);
}

}
