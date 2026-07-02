#ifndef HPP_GUARD_PLEXUS_NATIVE_WIN_SHM_REGION_BROKER_H
#define HPP_GUARD_PLEXUS_NATIVE_WIN_SHM_REGION_BROKER_H

#include "plexus/native/win_region_handle.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/detail/compat.h"

#include <cstddef>
#include <string_view>

namespace plexus::native {

class win_shm_region_broker
{
public:
    using region_handle = plexus::native::win_region_handle;

    // A create above this fast-fails with too_large before any syscall, a bound
    // against a local memory-exhaustion request.
    static constexpr std::size_t k_max_region_size = 1u << 30;

    win_shm_region_broker() = default;

    win_shm_region_broker(const win_shm_region_broker &)            = delete;
    win_shm_region_broker &operator=(const win_shm_region_broker &) = delete;
    win_shm_region_broker(win_shm_region_broker &&)                 = delete;
    win_shm_region_broker &operator=(win_shm_region_broker &&)      = delete;

    // Named file mappings are refcounted and auto-freed on last-handle-close, so
    // the returned handle only unmaps + closes; opts.unlink_stale_on_create is a
    // no-op (there is no /dev/shm-style orphan to reclaim).
    plexus::shm::region_status create(std::string_view name, std::size_t bytes, const plexus::shm::create_options &opts, region_handle &out);

    // Attach is the trust boundary: it consults the attach policy after name
    // sanitization and before opening; a false result returns denied.
    plexus::shm::region_status attach(std::string_view name, region_handle &out);

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> policy);

private:
    // The native mapping HANDLE is passed as void* so <windows.h> stays out of
    // this header; both helpers construct the friended win_region_handle.
    static plexus::shm::region_status finish_create_map(void *mapping, std::size_t length, region_handle &out);
    static plexus::shm::region_status finish_attach_map(void *mapping, region_handle &out);

    plexus::detail::move_only_function<bool(std::string_view)> m_attach_policy{[](std::string_view) { return true; }};
};

}

static_assert(::plexus::shm::region_broker<::plexus::native::win_shm_region_broker>, "win_shm_region_broker must satisfy the core region_broker concept");

#endif
