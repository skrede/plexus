#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARD_GATE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARD_GATE_H

#include "plexus/node_id.h"

#include "plexus/wire/forwarded_frame.h"
#include "plexus/wire/udp_dedup_window.h"

#include <map>
#include <cstddef>
#include <cstdint>
#include <compare>

namespace plexus::io::detail {

// The forward-admission verdict. Only `admit` mutates the dedup window; each drop is a distinct value
// so a caller counts loop/hop/duplicate/too_old rejections apart rather than collapsing them.
enum class forward_admission : std::uint8_t
{
    admit,
    drop_loop,
    drop_hop,
    drop_duplicate,
    drop_too_old
};

// The delivery-edge dedup key: (origin, arrival-relay). Keying on the arriving relay as well as the
// origin lets two relays mint independent seq streams for the same origin without colliding, and
// measures a duplicate only against the relay that carried the first sighting.
struct forward_dedup_key
{
    node_id origin;
    node_id arrival_relay;

    auto operator<=>(const forward_dedup_key &) const = default;
};

// The node-wide table of per-(origin, arrival-relay) sequence windows. A first sighting mints a window
// at the configured depth; udp_dedup_window then anchors it on the arriving seq (its first-sighting
// anchor), so a late-joining origin counter never misclassifies as too_old.
class forward_dedup_table
{
public:
    wire::udp_dedup_window::outcome admit(const node_id &origin, const node_id &arrival_relay, std::uint16_t seq, std::size_t depth)
    {
        auto [it, _] = m_windows.try_emplace(forward_dedup_key{origin, arrival_relay}, depth);
        return it->second.admit(seq);
    }

    std::size_t tracked() const noexcept
    {
        return m_windows.size();
    }

private:
    std::map<forward_dedup_key, wire::udp_dedup_window> m_windows;
};

// The pure forward-admission chain, in the peer_report receive-gate order: self/loop, then hop budget,
// then per-(origin, arrival-relay) dedup. The window is consulted LAST and mutates only on a fresh seq,
// so a loop- or hop-rejected frame neither creates nor advances a window — no state mutation on reject.
// No I/O, no engine, no session: unit-testable in isolation.
inline forward_admission forward_gate(const wire::forwarded_frame &ff, const node_id &local_id, const node_id &arrival_relay, std::uint8_t hop_budget, std::size_t dedup_depth,
                                      forward_dedup_table &dedup)
{
    // origin == arrival_relay is a loop ONLY for an unaddressed (pub/sub) frame — a relay re-wrapping its
    // own direct traffic. An addressed (request/response) frame legitimately has origin == sender at its
    // first hop: the caller wraps its OWN request onto the relay, and the responder its OWN reply.
    const bool addressed = ff.destination != node_id{};
    if(ff.origin == local_id || arrival_relay == local_id || (!addressed && ff.origin == arrival_relay))
        return forward_admission::drop_loop;
    if(ff.hop > hop_budget)
        return forward_admission::drop_hop;
    switch(dedup.admit(ff.origin, arrival_relay, ff.seq, dedup_depth))
    {
        case wire::udp_dedup_window::outcome::fresh:
            return forward_admission::admit;
        case wire::udp_dedup_window::outcome::duplicate:
            return forward_admission::drop_duplicate;
        case wire::udp_dedup_window::outcome::too_old:
            return forward_admission::drop_too_old;
    }
    return forward_admission::drop_duplicate;
}

}

#endif
