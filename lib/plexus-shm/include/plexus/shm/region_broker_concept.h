#ifndef HPP_GUARD_PLEXUS_SHM_REGION_BROKER_CONCEPT_H
#define HPP_GUARD_PLEXUS_SHM_REGION_BROKER_CONCEPT_H

#include "plexus/detail/compat.h"

#include <cstddef>
#include <cstdint>
#include <concepts>
#include <string_view>

namespace plexus::shm {

// A broker maps the irreducible POSIX errors onto this closed set; core never sees
// an errno.
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

// unlink_stale_on_create reclaims a crashed creator's orphaned name before the create
// (bounded, not robust dead-peer arbitration). The 0600 owner-only perms default keeps
// a region attachable ONLY by the same uid (the access-control floor).
struct create_options
{
    bool unlink_stale_on_create = false;
    std::uint32_t perms         = 0600;
};

// The region create/attach seam, defined in core so no core translation unit pulls a
// POSIX memory-mapping header. Both verbs return a region_status by value and a handle
// through an out-param, mirroring the loan/take convention. set_attach_policy injects a
// cold-path predicate consulted before an attach opens a region (default-allow today).
template<typename T>
concept region_broker = requires(T &broker, std::string_view name, std::size_t bytes, const create_options &opts, typename T::region_handle &out,
                                 plexus::detail::move_only_function<bool(std::string_view)> policy) {
    typename T::region_handle;
    { broker.create(name, bytes, opts, out) } -> std::same_as<region_status>;
    { broker.attach(name, out) } -> std::same_as<region_status>;
    broker.set_attach_policy(std::move(policy));
};

}

#endif
