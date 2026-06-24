#ifndef HPP_GUARD_PLEXUS_NATIVE_POSIX_SHM_REGION_BROKER_H
#define HPP_GUARD_PLEXUS_NATIVE_POSIX_SHM_REGION_BROKER_H

#include "plexus/native/region_handle.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <string>
#include <cstddef>
#include <string_view>

namespace plexus::native {

// POSIX shared-memory backend satisfying the core region_broker concept.
// Synchronous (no io_context/strand): create allocates + maps a named region,
// attach maps an existing one. The OS primitives (shm_open/mmap/ftruncate/
// shm_unlink) stay hidden behind region_handle; core never pulls a POSIX
// memory-mapping header. Non-copy/non-move (the house default for a broker).
//
// The verbs return the core plexus::shm::region_status by value and the mapped
// region through an out-param, mirroring the loan/take out-param convention;
// the backend-internal shm_error (the errno mapping) never escapes.
class posix_shm_region_broker
{
public:
    // The associated handle the concept names (typename T::region_handle).
    using region_handle = plexus::native::region_handle;

    // Per-region size policy ceiling. A create request above this fast-fails
    // with too_large BEFORE any syscall -- a deliberate, documented bound
    // against a local memory-exhaustion request, not an arithmetic-overflow
    // artifact. 1 GiB is a generous-but-bounded same-host region.
    static constexpr std::size_t k_max_region_size = 1u << 30;

    posix_shm_region_broker() = default;

    posix_shm_region_broker(const posix_shm_region_broker &)            = delete;
    posix_shm_region_broker &operator=(const posix_shm_region_broker &) = delete;
    posix_shm_region_broker(posix_shm_region_broker &&)                 = delete;
    posix_shm_region_broker &operator=(posix_shm_region_broker &&)      = delete;

    // Allocates a named region of at least `bytes` usable bytes (page-rounded)
    // and maps it writable. bytes==0 returns failed; a request above
    // k_max_region_size returns too_large before any syscall. opts.perms is the
    // creation mode (0600 owner-only default, the same-uid access floor);
    // opts.unlink_stale_on_create reclaims a crashed creator's orphan once. On
    // ok, `out` owns the name and unlinks it on release.
    plexus::shm::region_status create(std::string_view name, std::size_t bytes,
                                          const plexus::shm::create_options &opts,
                                          region_handle                         &out);

    // Maps an existing named region writable. The returned handle is an
    // attacher: it munmaps on release but never unlinks. The attach path is the
    // trust boundary, so it consults the attach policy after name sanitization
    // and before opening; a false result returns denied.
    plexus::shm::region_status attach(std::string_view name, region_handle &out);

    // Injection point for an attach-time authorization check. Default-allow; a
    // stricter-than-uid policy can be supplied with no signature churn. A
    // move-only predicate (the project move-only-callback convention).
    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy);

private:
    // Size the just-created region and map it, assembling the owning handle; on failure the fd is
    // closed AND the region unlinked (the creator owns the name). The attach mirror maps an
    // existing region's stat'd size and never unlinks. Both relocated out of create()/attach() so
    // each verb stays within the function ceiling.
    static plexus::shm::region_status finish_create(int fd, std::size_t length,
                                                        std::string canonical, region_handle &out);
    static plexus::shm::region_status finish_attach(int fd, std::string canonical,
                                                        region_handle &out);

    plexus::detail::move_only_function<bool(std::string_view)> m_attach_policy{[](std::string_view)
                                                                               { return true; }};
};

}

static_assert(::plexus::shm::region_broker<::plexus::native::posix_shm_region_broker>,
              "posix_shm_region_broker must satisfy the core region_broker concept");

#endif
