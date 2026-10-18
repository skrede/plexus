#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H

#include "plexus/detail/compat.h"

#include <array>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::io {

// Why a same-host lane is declined, demoted or lost; an engaged lane carries none. Only transient is worth a later
// attempt. A demoted lane stays engaged on its secondary wake. Values are append-only: they index the counters.
enum class fallback_reason : std::uint8_t
{
    none,
    transient,
    retries_exhausted,
    incompatible_layout,
    slab_ceiling,
    os_allocator,
    notifier_denied,
    notifier_unsupported,
    notifier_exhausted,
    consumer_capacity,
    not_found,
    no_space,
    waiter_lost,
    notifier_demoted_denied,
    notifier_demoted_unsupported,
    notifier_demoted_exhausted,
};

inline constexpr std::size_t k_fallback_reason_count = static_cast<std::size_t>(fallback_reason::notifier_demoted_exhausted) + 1;

// How often each reason struck, indexed by the reason.
using fallback_counts = std::array<std::uint64_t, k_fallback_reason_count>;

// The transport-neutral mint result the engine's upgrade coordinator retains. A null channel
// signals the gate DECLINED — the pair keeps the wire. The coordinator retains the channel
// single-owner. fits is the per-message route predicate the minting medium constructs: true
// when a message rides the companion channel, false when it falls back to the wire. The generic
// seam never interprets the medium's geometry — it only calls fits.
template<typename Channel>
struct upgrade_mint
{
    upgrade_mint() noexcept
            : reason(fallback_reason::none)
    {
    }

    upgrade_mint(std::unique_ptr<Channel> minted, plexus::detail::move_only_function<bool(std::size_t)> route) noexcept
            : reason(fallback_reason::none)
            , channel(std::move(minted))
            , fits(std::move(route))
    {
    }

    explicit upgrade_mint(fallback_reason declined) noexcept
            : reason(declined)
    {
    }

    fallback_reason reason;
    std::unique_ptr<Channel> channel;
    plexus::detail::move_only_function<bool(std::size_t)> fits;
};

// A type-erased OWNER of the live receive companion: never called, it exists only to hold the
// concrete RAII handle by move, so dropping it runs the handle's destructor (release the ring).
// An empty owner signals the gate DECLINED — the pair then has only the wire receive path. The
// erasure keeps the coordinator decoupled from the concrete member's handle type.
struct upgrade_receive
{
    upgrade_receive() noexcept
            : reason(fallback_reason::none)
    {
    }

    upgrade_receive(plexus::detail::move_only_function<void()> handle) noexcept
            : reason(fallback_reason::none)
            , owner(std::move(handle))
    {
    }

    explicit upgrade_receive(fallback_reason declined) noexcept
            : reason(declined)
    {
    }

    fallback_reason reason;
    plexus::detail::move_only_function<void()> owner;

    bool engaged() const noexcept
    {
        return static_cast<bool>(owner);
    }
};

}

#endif
