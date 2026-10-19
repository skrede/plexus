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
// attempt. A demoted lane stays engaged on its secondary wake. not_offered: the publisher's response withheld its ring;
// withdrawn: a subscriber lost the receive lane it had answered ready; liveness_unavailable: the host offers no proof
// that a reader is alive. Values are append-only: they index the counters.
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
    not_offered,
    withdrawn,
    liveness_unavailable,
};

inline constexpr std::size_t k_fallback_reason_count = static_cast<std::size_t>(fallback_reason::liveness_unavailable) + 1;

// How often each reason struck, indexed by the reason.
using fallback_counts = std::array<std::uint64_t, k_fallback_reason_count>;

// The receiving medium names no reader slot, so the publisher has none to place.
inline constexpr std::uint32_t k_no_reader = 0xFFFFFFFFu;

// The transport-neutral mint result the engine's upgrade coordinator retains. A null channel
// signals the gate DECLINED — the pair keeps the wire. The coordinator retains the channel
// single-owner. fits is the per-message route predicate the minting medium constructs: true
// when a message rides the companion channel, false when it falls back to the wire. The generic
// seam never interprets the medium's geometry — it only calls fits. place(reader) starts a
// subscriber's reader at the lane's newest message when the publisher switches it; a medium
// with no readers to place leaves it empty.
template<typename Channel>
struct upgrade_mint
{
    upgrade_mint() noexcept
            : reason(fallback_reason::none)
    {
    }

    upgrade_mint(std::unique_ptr<Channel> minted, plexus::detail::move_only_function<bool(std::size_t)> route,
                 plexus::detail::move_only_function<bool(std::uint32_t)> placer = {}) noexcept
            : reason(fallback_reason::none)
            , channel(std::move(minted))
            , fits(std::move(route))
            , place(std::move(placer))
    {
    }

    explicit upgrade_mint(fallback_reason declined) noexcept
            : reason(declined)
    {
    }

    fallback_reason reason;
    std::unique_ptr<Channel> channel;
    plexus::detail::move_only_function<bool(std::size_t)> fits;
    plexus::detail::move_only_function<bool(std::uint32_t)> place;
};

// Where one message for one subscriber goes, and the reader slot its ready named: a null lane keeps the pair on the wire;
// otherwise fits says whether this message rides the lane, which the subscriber reads once it has switched to it.
template<typename Channel>
struct lane_route
{
    Channel *lane;
    bool fits;
    std::uint32_t reader;
};

// The type-erased OWNER of the live receive companion, which holds it until a switch marker resumes it: calling it with
// the marker's sequence starts delivery there, and dropping it runs the handle's destructor (release the ring). reader is
// the slot the subscriber's ready names for the publisher to place. An empty owner signals the gate DECLINED; the pair
// then has only the wire receive path. The erasure keeps the coordinator decoupled from the concrete member's handle type.
struct upgrade_receive
{
    upgrade_receive() noexcept
            : reason(fallback_reason::none)
            , reader(k_no_reader)
    {
    }

    upgrade_receive(plexus::detail::move_only_function<void(std::uint64_t)> resume, std::uint32_t slot = k_no_reader) noexcept
            : reason(fallback_reason::none)
            , reader(slot)
            , resume_cb(std::move(resume))
    {
    }

    explicit upgrade_receive(fallback_reason declined) noexcept
            : reason(declined)
            , reader(k_no_reader)
    {
    }

    fallback_reason reason;
    std::uint32_t reader;
    plexus::detail::move_only_function<void(std::uint64_t)> resume_cb;

    bool engaged() const noexcept
    {
        return static_cast<bool>(resume_cb);
    }
};

}

#endif
