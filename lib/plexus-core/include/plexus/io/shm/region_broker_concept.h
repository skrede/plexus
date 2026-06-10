#ifndef HPP_GUARD_PLEXUS_IO_SHM_REGION_BROKER_CONCEPT_H
#define HPP_GUARD_PLEXUS_IO_SHM_REGION_BROKER_CONCEPT_H

#include "plexus/detail/compat.h"

#include <cstddef>
#include <cstdint>
#include <concepts>
#include <string_view>

namespace plexus::io::shm {

// The status a broker create/attach returns alongside (or instead of) a region
// handle. Leads with ok exactly as loan_status / io_error do, so the status
// families read identically at every call site. A broker maps the irreducible
// POSIX errors (the name exists, the perms deny, the region is too large, a
// foreign/corrupt layout) onto this closed set; core never sees an errno.
//   already_exists -> create() found the name live; the caller re-issues attach().
//   not_found      -> attach() found no region under the name.
//   denied         -> the attach policy (or the 0600 perms) refused the open.
//   too_large      -> the requested size exceeds the broker's region ceiling.
//   incompatible   -> attach() found a region whose header layout is not ours.
//   failed         -> any other irreducible mapping failure.
enum class region_status : std::uint8_t
{
    ok,
    already_exists,
    not_found,
    denied,
    too_large,
    incompatible,
    failed,
};

// The options a create() carries — the cold-path policy the backend honors with a
// syscall. unlink_stale_on_create reclaims a crashed creator's orphaned name before
// the create (bounded — not robust dead-peer arbitration, which is deferred). perms
// is the POSIX mode the region is created with; the 0600 owner-only default keeps a
// same-host region attachable ONLY by the same uid (the access-control floor,
// mirroring the AF_UNIX 0700 precedent). Plain fields with defaults: no negotiation.
struct create_options
{
    bool          unlink_stale_on_create = false;
    std::uint32_t perms                  = 0600;
};

// The broker seam: the region create/attach surface the ring + registry borrow BY
// REFERENCE, defined in core so no core translation unit ever pulls a POSIX
// memory-mapping header. The compiled plexus-shm backend SATISFIES it with the
// shm-open / memory-map bodies,
// typed against an associated region_handle the backend provides (a move-only RAII
// mapping). create() mints a region under a deterministic name (region_name_for);
// attach() opens an existing one. Both return BOTH a region_status (by value) and a
// handle through an out-param, mirroring the loan/take out-param convention.
// set_attach_policy injects a cold-path predicate consulted before an attach opens a
// region — the seam for a stricter-than-uid policy later (default-allow today).
//
// The verbs are bare-call-expression typed (the byte_channel / transport_backend
// idiom): the create/attach verbs are constrained with -> std::same_as<region_status>,
// the predicate install is a bare call. Core holds ONLY this concept; the mechanism
// is the backend's.
template <typename T>
concept region_broker = requires(T &broker,
                                 std::string_view name,
                                 std::size_t bytes,
                                 const create_options &opts,
                                 typename T::region_handle &out,
                                 plexus::detail::move_only_function<bool(std::string_view)> policy)
{
    typename T::region_handle;
    { broker.create(name, bytes, opts, out) } -> std::same_as<region_status>;
    { broker.attach(name, out) }              -> std::same_as<region_status>;
    broker.set_attach_policy(std::move(policy));
};

}

#endif
