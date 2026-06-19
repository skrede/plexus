#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINKS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINKS_H

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/routing_dispatch.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

// The engine's posted observation sinks, factored out of the engine body. Each is a
// value-capturing callable that fans out on the BORROWED executor over a snapshot, never
// synchronously from the site (the per-packet-inline-fire DoS guard). The data-path sinks
// SHORT-CIRCUIT through the capture gate before posting: payload sinks consult should_emit(hash)
// (selection AND observer-presence behind one branch), event-record sinks read observers_present()
// directly — an unobserved/unselected node pays one branch and allocates nothing. RELOCATION: the
// engine still owns m_capture/m_executor/m_observers; these carry the engine by reference and call
// its private post helpers (they are friends).

template<typename Engine>
[[nodiscard]] auto make_drop_sink(Engine &e)
{
    return [&e](const drop_event &ev) { post_drop(e, ev); };
}

// The on_wire tap, shaped like the drop sink so the decorator stays recorder-agnostic. Each frame
// posts via post_wire — the io->executor crossing that keeps the ring single-writer. The decorator
// stamps the per-direction sequence; the peer identity is bound at the channel-mint point, so the
// wire_record carries both offline cross-node loss-join keys.
template<typename Engine>
[[nodiscard]] auto make_wire_sink(Engine &e, const node_id &peer)
{
    return [&e, peer](recording::wire_direction dir, std::uint64_t seq,
                      std::span<const std::byte> bytes) { post_wire(e, dir, seq, peer, bytes); };
}

template<typename Engine>
[[nodiscard]] auto make_publish_sink(Engine &e)
{
    using policy_type = typename Engine::policy_type;
    return [&e](std::uint64_t hash, std::string_view fqn, const message_view &v)
    {
        if(!e.m_capture.should_emit(hash))
            return;
        policy_type::post(e.m_executor, [&e, fqn = std::string{fqn}, v]
                          { fan_out(e, [&](observer &o) { o.on_message_published(fqn, v); }); });
    };
}

template<typename Engine>
[[nodiscard]] auto make_deliver_sink(Engine &e)
{
    using policy_type = typename Engine::policy_type;
    return [&e](std::uint64_t hash, std::string_view fqn, const message_info &info,
                const message_view &v)
    {
        if(!e.m_capture.should_emit(hash))
            return;
        policy_type::post(
                e.m_executor, [&e, fqn = std::string{fqn}, info, v]
                { fan_out(e, [&](observer &o) { o.on_message_delivered(fqn, info, v); }); });
    };
}

template<typename Engine>
[[nodiscard]] auto make_qos_change_sink(Engine &e)
{
    using policy_type = typename Engine::policy_type;
    return [&e](const qos_change_event &ev)
    {
        if(e.m_capture.observers_present() == 0)
            return;
        policy_type::post(e.m_executor,
                          [&e, ev] { fan_out(e, [&](observer &o) { o.on_qos_change(ev); }); });
    };
}

template<typename Engine, typename Edge>
[[nodiscard]] auto make_rpc_sink(Engine &e, Edge edge)
{
    using policy_type = typename Engine::policy_type;
    return [&e, edge](std::string_view fqn, const auto &v)
    {
        if(e.m_capture.observers_present() == 0)
            return;
        policy_type::post(e.m_executor, [&e, edge, fqn = std::string{fqn}, v]
                          { fan_out(e, [&](observer &o) { (o.*edge)(fqn, v); }); });
    };
}

}

#endif
