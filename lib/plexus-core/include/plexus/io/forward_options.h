#ifndef HPP_GUARD_PLEXUS_IO_FORWARD_OPTIONS_H
#define HPP_GUARD_PLEXUS_IO_FORWARD_OPTIONS_H

#include "plexus/match/key_pattern.h"
#include "plexus/match/detail/match_engine.h"

#include "plexus/wire/udp_dedup_window.h"

#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// Splice buffer ownership as a profile/QoS knob. pooled_owned_copy (the default) copies the payload
// into a pre-sized pool slot at the splice — one bounded copy, zero steady-state allocation, simplest
// cross-session ownership. refcounted_zero_copy retains an owner-carrying inbound view instead, for
// bandwidth-tight host relays. The knob is honored, never silently reduced to pooled-only.
enum class splice_ownership : std::uint8_t
{
    pooled_owned_copy,
    refcounted_zero_copy
};

// The forwarding relay's loop-safety and egress bounds plus the splice-pool sizing. Every numeric is a
// defaulted struct field swept on target later, never pinned in the mechanism; scope_pattern, when set,
// scopes which topics the relay forwards. ownership carries the D95.1 knob (default pooled_owned_copy).
struct forward_options
{
    std::uint8_t hop_budget      = 8;
    std::size_t dedup_depth      = wire::udp_dedup_window::depth_max;
    std::size_t queue_depth      = 256;
    std::size_t splice_pool_slots = 64;
    std::size_t splice_slot_bytes = 2048;
    std::string scope_pattern;
    splice_ownership ownership = splice_ownership::pooled_owned_copy;
};

// The parse-once derivative the splice holds: the carried numerics, the compiled forward-scope pattern
// (absent = forward every subscribed topic), and the honored ownership mode. A misconfigured scope
// fails closed to scope_none so a bad label forwards nothing rather than everything.
struct forward_ctx
{
    std::uint8_t hop_budget      = 8;
    std::size_t dedup_depth      = wire::udp_dedup_window::depth_max;
    std::size_t queue_depth      = 256;
    std::size_t splice_pool_slots = 64;
    std::size_t splice_slot_bytes = 2048;
    splice_ownership ownership   = splice_ownership::pooled_owned_copy;
    bool scope_all               = true;
    std::optional<match::key_pattern> scope;
};

inline forward_ctx make_forward_ctx(const forward_options &opts)
{
    forward_ctx ctx;
    ctx.hop_budget       = opts.hop_budget;
    ctx.dedup_depth      = opts.dedup_depth;
    ctx.queue_depth      = opts.queue_depth;
    ctx.splice_pool_slots = opts.splice_pool_slots;
    ctx.splice_slot_bytes = opts.splice_slot_bytes;
    ctx.ownership        = opts.ownership;
    if(!opts.scope_pattern.empty())
    {
        ctx.scope_all = false;
        if(auto p = match::key_pattern::make(opts.scope_pattern))
            ctx.scope = *p;
    }
    return ctx;
}

}

#endif
