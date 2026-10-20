#ifndef HPP_GUARD_PLEXUS_IO_COMPANION_EVENT_H
#define HPP_GUARD_PLEXUS_IO_COMPANION_EVENT_H

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

// What one end of a co-host pair tells the other about the shared-memory lane of one topic: the publisher's subscribe
// response offers its ring, withholds it (not_offered) or names no publisher, and a ring that appears later is offered
// on its own; the subscriber answers that its receive companion is ready, and later, if it loses it, that it was
// withdrawn.
enum class companion_event : std::uint8_t
{
    offered,
    not_offered,
    ready,
    withdrawn,
    no_publisher,
};

// not_offered and no_publisher ride only the subscribe response; every other event has its own verb.
inline std::optional<wire::msg_type> verb_of(companion_event event) noexcept
{
    if(event == companion_event::ready)
        return wire::msg_type::companion_ready;
    if(event == companion_event::withdrawn)
        return wire::msg_type::companion_withdrawn;
    if(event == companion_event::offered)
        return wire::msg_type::companion_offer;
    return std::nullopt;
}

inline wire::offer_state offer_state_of(companion_event event) noexcept
{
    if(event == companion_event::offered)
        return wire::offer_state::offered;
    return event == companion_event::no_publisher ? wire::offer_state::no_publisher : wire::offer_state::withheld;
}

inline companion_event offer_event_of(wire::offer_state state) noexcept
{
    if(state == wire::offer_state::offered)
        return companion_event::offered;
    return state == wire::offer_state::withheld ? companion_event::not_offered : companion_event::no_publisher;
}

}

#endif
