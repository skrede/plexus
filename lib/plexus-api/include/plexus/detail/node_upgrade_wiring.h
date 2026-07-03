#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_UPGRADE_WIRING_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_UPGRADE_WIRING_H

#include "plexus/io/endpoint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include <span>
#include <string>
#include <memory>
#include <cstddef>
#include <utility>
#include <type_traits>
#include <string_view>

namespace plexus::detail {

// Whether a member exposes the same-host ring-acquire probe.
template<typename M>
constexpr bool node_has_can_acquire = requires(M &m) {
    { m.can_acquire(std::declval<const io::endpoint &>()) } -> std::convertible_to<bool>;
};

// The if constexpr keeps a single-transport build from instantiating the erasure when no shm member
// reaches here.
template<typename EngineChannel, typename C>
std::unique_ptr<EngineChannel> wrap_companion(std::unique_ptr<C> ch)
{
    if constexpr(std::is_same_v<EngineChannel, io::polymorphic_byte_channel>)
        return std::make_unique<EngineChannel>(std::make_unique<io::channel_adapter<C>>(std::move(ch)));
    else
        return ch;
}

// The if constexpr is load-bearing: companion_selection_hook only instantiates for a can_acquire
// leaf, so a non-shm leaf never forces an uncompilable companion_selection_hook<that-leaf>.
template<typename M>
void bind_shm_hook(io::selection_hook &hook, M &member)
{
    if constexpr(node_has_can_acquire<M>)
        hook = member.companion_selection_hook();
}

// Captures the shm member by reference; it outlives the node-owned glue. Precondition: at least one
// member is shm-bearing.
template<typename... Transports>
io::selection_hook shm_preference_hook(Transports &...transports)
{
    io::selection_hook hook = io::first_candidate{};
    (bind_shm_hook(hook, transports), ...);
    return hook;
}

// When the pack carries an shm member, prefer shm when the ring acquires, else AF_UNIX, so a
// multi-candidate local tier resolves by runtime acquire, not positional order. can_acquire is
// mode-aware: a wire_fallback topic declines the ring, its same-host channel staying the wire.
template<typename... Transports>
io::selection_hook resolve_hook([[maybe_unused]] Transports &...transports)
{
    if constexpr((node_has_can_acquire<Transports> || ...))
        return shm_preference_hook(transports...);
    else
        return io::first_candidate{};
}

// Install the companion-ring mint and receive gates for one shm member into the engine's
// coordinator, capturing the member by reference (it outlives the node-owned glue). The minted
// concrete channel is wrapped into the engine's byte_channel before the coordinator retains it.
template<typename Engine, typename Member>
// NOLINTNEXTLINE(readability-function-size)
void install_same_host_upgrade(Engine &engine, Member &member)
{
    using engine_channel = typename Engine::channel_type;
    engine.on_upgrade_gate(
            [&member](std::string_view fqn) -> io::upgrade_mint<engine_channel>
            {
                const std::string key{fqn};
                std::unique_ptr<typename Member::channel_type> ch = member.mint_companion(key);
                if(!ch)
                    return {};
                auto fits = member.companion_route(key);
                return {wrap_companion<engine_channel>(std::move(ch)), std::move(fits)};
            });
    // Each drained frame is injected into the matching peer session, resolved by node_name. The
    // drain posts on the node's executor, so inject runs on an executor turn with no drain thread.
    // The returned owner is never called: dropping it on peer-dead runs the RAII handle's dtor
    // (clear sink, release ring). It captures the engine by reference (it outlives the coordinator).
    // A null handle declines and the subscriber keeps the wire.
    engine.on_upgrade_receive_gate(
            [&member, &engine](std::string_view node_name, std::string_view fqn) -> io::upgrade_receive
            {
                const std::string key{fqn};
                const std::string peer{node_name};
                auto handle = member.mint_receive_companion(key, [&engine, peer](std::span<const std::byte> frame) { engine.inject_upgrade_receive(peer, frame); });
                if(!handle)
                    return {};
                return {[h = std::move(handle)]() mutable { (void)h; }};
            });
}

}

#endif
