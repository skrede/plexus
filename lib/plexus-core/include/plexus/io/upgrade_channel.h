#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_CHANNEL_H

#include "plexus/io/shm/ring_geometry_mode.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <cstdint>

namespace plexus::io {

// The transport-neutral mint result the engine's upgrade coordinator retains. A null channel
// signals the gate DECLINED — the pair keeps the wire. The coordinator retains the channel
// single-owner; mode + slot_capacity are the per-message route inputs. The mode is carried
// opaquely as the upgrade-capable transport's own enum (an shm member fills it with its ring
// geometry mode); the generic seam does not interpret it beyond the per-message route.
template<typename Channel>
struct upgrade_mint
{
    std::unique_ptr<Channel> channel;
    shm::ring_geometry_mode  mode          = shm::ring_geometry_mode::reliable_preserving;
    std::uint64_t            slot_capacity = 0;
};

// A type-erased OWNER of the live receive companion: never called, it exists only to hold the
// concrete RAII handle by move, so dropping it runs the handle's destructor (release the ring).
// An empty owner signals the gate DECLINED — the pair then has only the wire receive path. The
// erasure keeps the coordinator decoupled from the concrete member's handle type.
struct upgrade_receive
{
    plexus::detail::move_only_function<void()> owner;

    [[nodiscard]] bool engaged() const noexcept { return static_cast<bool>(owner); }
};

}

#endif
