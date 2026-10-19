#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_UPGRADE_WIRING_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_UPGRADE_WIRING_H

#include "plexus/io/endpoint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/wire/detail/data_frame_codec.h"

#include <span>
#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
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

// A lost ring waiter drops every lane the coordinator holds for the fqn and withdraws each receive lane from its
// publisher, so each pair keeps the wire. The engine outlives the coordinator and therefore every lane that carries this.
template<typename Engine>
plexus::detail::move_only_function<void()> drop_lanes_on_loss(Engine &engine, std::string fqn)
{
    return [&engine, fqn = std::move(fqn)] { engine.coordinator().on_medium_lost(fqn); };
}

template<typename Engine, typename Member>
io::upgrade_mint<typename Engine::channel_type> mint_send_lane(Engine &engine, Member &member, std::string_view fqn)
{
    const std::string key{fqn};
    std::unique_ptr<typename Member::channel_type> ch = member.mint_companion(key, drop_lanes_on_loss(engine, key));
    if(!ch)
        return io::upgrade_mint<typename Engine::channel_type>{member.last_fallback_reason()};
    auto fits = member.companion_route(key);
    return {wrap_companion<typename Engine::channel_type>(std::move(ch)), std::move(fits), member.companion_place(key)};
}

// Each frame from the marker's sequence on is injected into the matching peer session, resolved by node_name, on an
// executor turn with no drain thread; the frames below it reached the subscriber over the wire.
template<typename Engine>
plexus::detail::move_only_function<void(std::span<const std::byte>)> deliver_from(Engine &engine, std::string peer, std::uint64_t first_sequence)
{
    return [&engine, peer = std::move(peer), first_sequence](std::span<const std::byte> frame)
    {
        const std::optional<std::uint64_t> sequence = wire::framed_sequence(frame);
        if(sequence && *sequence >= first_sequence)
            engine.inject_upgrade_receive(peer, frame);
    };
}

// The publisher's ring is named by the identity its session proved, never by the slot key, which on an accepted session
// is provisional; a session not yet complete declines as transient. The handle takes nothing and gates nothing until the
// publisher places its reader slot and its marker resumes it. Dropping the owner on peer-dead runs the RAII handle's dtor
// (clear sink, release ring). A null handle declines with the member's reason and the subscriber keeps the wire.
template<typename Engine, typename Member>
io::upgrade_receive mint_receive_lane(Engine &engine, Member &member, std::string_view node_name, std::string_view fqn)
{
    const auto *session = engine.registry().session_for_name(node_name);
    if(session == nullptr || !session->is_complete())
        return io::upgrade_receive{io::fallback_reason::transient};
    const std::string key{fqn};
    auto handle = member.mint_receive_companion(key, session->peer_identity(), drop_lanes_on_loss(engine, key));
    if(!handle)
        return io::upgrade_receive{member.last_fallback_reason()};
    const std::uint32_t reader = handle->reader_slot();
    return io::upgrade_receive{[&engine, peer = std::string{node_name}, h = std::move(handle)](std::uint64_t first_sequence) mutable
                               { h->resume(deliver_from(engine, peer, first_sequence)); }, reader};
}

// The member's registry reports each lost waiter and each notifier demotion, which the coordinator counts beside the
// declines it sees itself.
template<typename Engine, typename Member>
void count_member_fallbacks(Engine &engine, Member &member)
{
    member.registry().on_fallback([&engine](io::fallback_reason reason) { engine.coordinator().count_fallback(reason); });
}

// Install the companion-ring mint and receive gates for one shm member into the engine's coordinator, capturing the
// member and the engine by reference (both outlive the node-owned glue). The minted concrete channel is wrapped into
// the engine's byte_channel before the coordinator retains it.
template<typename Engine, typename Member>
void install_same_host_upgrade(Engine &engine, Member &member)
{
    engine.coordinator().on_gate([&engine, &member](std::string_view fqn) { return mint_send_lane(engine, member, fqn); });
    engine.coordinator().on_receive_gate([&engine, &member](std::string_view node_name, std::string_view fqn) { return mint_receive_lane(engine, member, node_name, fqn); });
    count_member_fallbacks(engine, member);
}

}

#endif
