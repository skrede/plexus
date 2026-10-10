#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_FORWARD_RPC_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_FORWARD_RPC_H

#include "plexus/io/route_select.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/io/detail/forward_splice.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>

namespace plexus::io::detail {

// A live session for a PROVEN identity — an accepted session's slot key is provisional, so the table
// is scanned by handshake-proven identity, mirroring relay_broadcast's loop guard.
template<typename Engine>
typename Engine::session_type *session_by_identity(Engine &e, const node_id &proven)
{
    typename Engine::session_type *found = nullptr;
    e.m_registry.for_each_connected([&](const node_id &, typename Engine::session_type &s) { if(s.peer_identity() == proven) found = &s; });
    return found;
}

template<typename Engine>
typename Engine::session_type *best_live_forward_session(Engine &e, std::span<const route_candidate> cands, const node_id &destination)
{
    using session_type = typename Engine::session_type;

    session_type *best_session   = nullptr;
    const route_candidate *chosen = nullptr;
    for(const route_candidate &c : cands)
    {
        if(!c.is_direct() && !c.reach.via)
            continue;
        session_type *s = session_by_identity(e, c.is_direct() ? destination : *c.reach.via);
        if(s != nullptr && (chosen == nullptr || route_outranks(c, *chosen)))
            chosen = &c, best_session = s;
    }
    return best_session;
}

// Resolve the session that carries the forwarded frame for the ranked pick. prefer_direct (and
// never, which route_select has already kept to a direct winner) binds the single pick's via and
// drops when it has no live session — a working direct path is used, but a stale pick never falls
// through. allow_relayed instead takes the best-ranked candidate that HAS a live session, so a
// held direct candidate with no session yields to a live relayed one. The scan is bounded to the
// candidate span, so the fall-through cannot loop.
template<typename Engine>
typename Engine::session_type *forward_session_for(Engine &e, std::span<const route_candidate> cands, std::size_t idx, const node_id &destination)
{
    if(e.m_route_options.usage == route_usage::allow_relayed)
        return best_live_forward_session(e, cands, destination);
    const route_candidate &c = cands[idx];
    if(!c.is_direct() && !c.reach.via)
        return nullptr;
    return session_by_identity(e, c.is_direct() ? destination : *c.reach.via);
}

template<typename Engine>
void send_forwarded_rpc(Engine &e, typename Engine::session_type &s, const node_id &origin, const node_id &destination, std::uint8_t hop, std::span<const std::byte> inner_frame)
{
    // Saturating so a max hop_budget config cannot wrap hop 255 -> 0 and reset the loop guard; the
    // gate rejects hop past the budget first, so the ceiling arm is normally unreached (defense only).
    const std::uint8_t out_hop = static_cast<std::uint8_t>(hop < 0xFF ? hop + 1 : hop);
    const std::size_t inner_n  = inner_frame.size() < wire::detail::k_forwarded_inner_max ? inner_frame.size() : wire::detail::k_forwarded_inner_max;
    e.m_forward_rpc_scratch.resize(wire::header_size + wire::detail::forwarded_frame_preamble_size + sizeof(std::uint32_t) + inner_n);
    const std::size_t n = detail::encode_forwarded_wire_into(e.m_forward_rpc_scratch, origin, destination, out_hop, e.m_forward_rpc_seq++, 0, inner_frame);
    s.rpc_peer().channel.send(std::span<const std::byte>{e.m_forward_rpc_scratch.data(), n});
}

// The identity re-resolution primitive: route_select over the destination's candidates, then
// re-wrap the header-on rpc frame in a forwarded envelope (origin preserved, destination re-stamped,
// hop+1) onto the chosen via-session. Stateless — no per-correlation table. With no awareness
// candidate a live direct session to the destination still carries the frame (a peer reached by a
// raw dial/accept holds a session but no candidate row); only a destination with neither a candidate
// nor a live session drop-with-count and returns false, and the caller's deadline fires the timeout.
template<typename Engine>
bool forward_rpc(Engine &e, const node_id &origin, const node_id &destination, std::uint8_t hop, std::span<const std::byte> inner_frame)
{
    using session_type = typename Engine::session_type;

    const auto cands      = e.m_known_peers.candidates(destination);
    const std::size_t idx = route_select(cands, e.m_route_options.usage);
    if(idx == route_select_npos)
    {
        if(session_type *direct = session_by_identity(e, destination))
            return send_forwarded_rpc(e, *direct, origin, destination, hop, inner_frame), true;
        return (void)++e.m_forward_rpc_dropped, false;
    }
    if(session_type *s = forward_session_for(e, cands, idx, destination))
        return send_forwarded_rpc(e, *s, origin, destination, hop, inner_frame), true;
    return (void)++e.m_forward_rpc_dropped, false;
}

}

#endif
