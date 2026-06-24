#ifndef HPP_GUARD_PLEXUS_WIRE_RPC_STATUS_H
#define HPP_GUARD_PLEXUS_WIRE_RPC_STATUS_H

#include <string>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// Wire-stable request/response status byte. Append-only: an integer is NEVER reordered or
// reused; a new status takes the next free integer. timeout and cancelled are reserved-but-not-
// produced in this release (no per-call timer, no caller cancel) so adding those behaviors later
// is additive, not a renumbering.
enum class rpc_status : std::uint8_t
{
    success             = 0,
    error               = 1,
    timeout             = 2,
    cancelled           = 3,
    no_handler          = 4,
    deserialize_failed  = 5,
    topic_not_found     = 8,
    peer_disconnected   = 18,
    rpc_response_orphan = 20,
};

// Pin each enumerator to its wire integer so a rename can never silently move a wire byte.
static_assert(static_cast<std::uint8_t>(rpc_status::success) == 0);
static_assert(static_cast<std::uint8_t>(rpc_status::error) == 1);
static_assert(static_cast<std::uint8_t>(rpc_status::timeout) == 2);
static_assert(static_cast<std::uint8_t>(rpc_status::cancelled) == 3);
static_assert(static_cast<std::uint8_t>(rpc_status::no_handler) == 4);
static_assert(static_cast<std::uint8_t>(rpc_status::deserialize_failed) == 5);
static_assert(static_cast<std::uint8_t>(rpc_status::topic_not_found) == 8);
static_assert(static_cast<std::uint8_t>(rpc_status::peer_disconnected) == 18);
static_assert(static_cast<std::uint8_t>(rpc_status::rpc_response_orphan) == 20);

// Optional rejection-boundary identity a consumer may surface alongside a status. A value type
// only — plexus keeps no exception in the core; the consumer decides whether to throw.
struct rpc_error_context
{
    std::optional<std::string> peer;
    std::optional<std::string> topic;
};

}

#endif
