#ifndef HPP_GUARD_PLEXUS_NATIVE_POSIX_SHM_REGION_BROKER_H
#define HPP_GUARD_PLEXUS_NATIVE_POSIX_SHM_REGION_BROKER_H

#include "plexus/native/region_handle.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <string>
#include <cstddef>
#include <string_view>

namespace plexus::native {

class posix_shm_region_broker
{
public:
    using region_handle = plexus::native::region_handle;

    // A create above this fast-fails with too_large before any syscall, a bound
    // against a local memory-exhaustion request.
    static constexpr std::size_t k_max_region_size = 1u << 30;

    posix_shm_region_broker() = default;

    posix_shm_region_broker(const posix_shm_region_broker &)            = delete;
    posix_shm_region_broker &operator=(const posix_shm_region_broker &) = delete;
    posix_shm_region_broker(posix_shm_region_broker &&)                 = delete;
    posix_shm_region_broker &operator=(posix_shm_region_broker &&)      = delete;

    // On ok, `out` owns the name and unlinks it on release;
    // opts.unlink_stale_on_create reclaims a crashed creator's orphan once.
    plexus::shm::region_status create(std::string_view name, std::size_t bytes, const plexus::shm::create_options &opts, region_handle &out);

    // The returned handle munmaps on release but never unlinks. Attach is the
    // trust boundary: it consults the attach policy after name sanitization and
    // before opening; a false result returns denied.
    plexus::shm::region_status attach(std::string_view name, region_handle &out);

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy);

private:
    // finish_create closes the fd AND unlinks the region on failure (the creator
    // owns the name); finish_attach never unlinks.
    static plexus::shm::region_status finish_create(int fd, std::size_t length, std::string canonical, region_handle &out);
    static plexus::shm::region_status finish_attach(int fd, std::string canonical, region_handle &out);

    plexus::detail::move_only_function<bool(std::string_view)> m_attach_policy{[](std::string_view) { return true; }};
};

}

static_assert(::plexus::shm::region_broker<::plexus::native::posix_shm_region_broker>, "posix_shm_region_broker must satisfy the core region_broker concept");

#endif
