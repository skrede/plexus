#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SHM_WIRING_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SHM_WIRING_H

#include "plexus/io/endpoint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/io/shm/shm_mux_member.h"

#include <span>
#include <string>
#include <memory>
#include <cstddef>
#include <utility>
#include <type_traits>
#include <string_view>

namespace plexus::detail {

// Whether a member exposes the same-host ring-acquire probe (the shm member).
template<typename M>
constexpr bool node_has_can_acquire = requires(M &m) {
    { m.can_acquire(std::declval<const io::endpoint &>()) } -> std::convertible_to<bool>;
};

// Wrap a minted concrete shm channel into the engine's byte_channel (the channel_adapter wrap,
// mirroring multiplexing_transport::wrap). The if-constexpr keeps a single-transport build from
// instantiating the erasure (no shm member reaches here).
template<typename EngineChannel, typename C>
std::unique_ptr<EngineChannel> wrap_companion(std::unique_ptr<C> ch)
{
    if constexpr(std::is_same_v<EngineChannel, io::polymorphic_byte_channel>)
        return std::make_unique<EngineChannel>(
                std::make_unique<io::channel_adapter<C>>(std::move(ch)));
    else
        return ch;
}

// Bind the hook over ONE leaf if it exposes the ring acquire probe. The if constexpr is
// load-bearing: prefer_upgradeable_hook only INSTANTIATES for a can_acquire leaf, so a non-shm
// leaf (AF_UNIX, TCP) never forces an uncompilable prefer_upgradeable_hook<that-leaf>.
template<typename M>
void bind_shm_hook(io::selection_hook &hook, M &member)
{
    if constexpr(node_has_can_acquire<M>)
        hook = io::prefer_upgradeable_hook(member);
}

// Build the hook over whichever borrowed member exposes the ring acquire probe, capturing it by
// reference (it outlives the node-owned glue). Precondition: at least one member is shm-bearing.
template<typename... Transports>
io::selection_hook shm_preference_hook(Transports &...transports)
{
    io::selection_hook hook = io::first_candidate{};
    (bind_shm_hook(hook, transports), ...);
    return hook;
}

// When the pack carries an shm member, install the same-host preference hook (prefer shm when the
// ring acquires, else AF_UNIX) so a >1-candidate local tier resolves by runtime acquire, not
// positional order. can_acquire is mode-aware: a wire_fallback topic declines the ring (its
// same-host channel is the wire — the too-large-message fail-safe). The if constexpr is
// load-bearing: shm_preference_hook only instantiates for an shm-bearing pack.
template<typename... Transports>
io::selection_hook resolve_hook(Transports &...transports)
{
    if constexpr((node_has_can_acquire<Transports> || ...))
        return shm_preference_hook(transports...);
    else
        return io::first_candidate{};
}

// Install the companion-ring MINT and RECEIVE gates for one shm member into the engine's
// coordinator, capturing the member by reference (it outlives the node-owned glue, as the
// preference hook does). The minted concrete channel is wrapped into the engine's byte_channel
// before the coordinator retains it. RELOCATION of the node's per-member install body.
template<typename Engine, typename Member>
// NOLINTNEXTLINE(readability-function-size)
void install_same_host_upgrade(Engine &engine, Member &member)
{
    using engine_channel = typename Engine::channel_type;
    engine.on_upgrade_gate(
            [&member](std::string_view fqn) -> io::upgrade_mint<engine_channel>
            {
                const std::string                              key{fqn};
                std::unique_ptr<typename Member::channel_type> ch = member.mint_companion(key);
                if(!ch)
                    return {};
                const auto g = member.resolved_geometry_for(key);
                return {wrap_companion<engine_channel>(std::move(ch)), g.mode, g.slot_capacity};
            });
    // The SUBSCRIBER-side receive gate: attach the co-host ring as a consumer and route each
    // drained frame into the matching peer session (resolved by node_name). The drain is posted on
    // the node's executor, so inject runs on an executor turn — no drain thread. The returned owner
    // is a closure holding the concrete RAII handle by move (never called — dropping it on
    // peer-dead runs the dtor: clear sink, release ring). It captures the engine by reference (it
    // outlives the coordinator). A null handle declines: the subscriber keeps the wire.
    engine.on_upgrade_receive_gate(
            [&member, &engine](std::string_view node_name,
                               std::string_view fqn) -> io::upgrade_receive
            {
                const std::string key{fqn};
                const std::string peer{node_name};
                auto              handle = member.mint_receive_companion(
                        key, [&engine, peer](std::span<const std::byte> frame)
                        { engine.inject_upgrade_receive(peer, frame); });
                if(!handle)
                    return {};
                return {[h = std::move(handle)]() mutable { (void)h; }};
            });
}

}

#endif
