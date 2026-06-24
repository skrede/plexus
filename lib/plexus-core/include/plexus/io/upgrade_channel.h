#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H

#include "plexus/detail/compat.h"

#include <memory>
#include <cstddef>

namespace plexus::io {

// The transport-neutral mint result the engine's upgrade coordinator retains. A null channel
// signals the gate DECLINED — the pair keeps the wire. The coordinator retains the channel
// single-owner. fits is the per-message route predicate the minting medium constructs: true
// when a message rides the companion channel, false when it falls back to the wire. The generic
// seam never interprets the medium's geometry — it only calls fits.
template<typename Channel>
struct upgrade_mint
{
    std::unique_ptr<Channel>                              channel;
    plexus::detail::move_only_function<bool(std::size_t)> fits;
};

// A type-erased OWNER of the live receive companion: never called, it exists only to hold the
// concrete RAII handle by move, so dropping it runs the handle's destructor (release the ring).
// An empty owner signals the gate DECLINED — the pair then has only the wire receive path. The
// erasure keeps the coordinator decoupled from the concrete member's handle type.
struct upgrade_receive
{
    plexus::detail::move_only_function<void()> owner;

    [[nodiscard]] bool engaged() const noexcept
    {
        return static_cast<bool>(owner);
    }
};

}

#endif
