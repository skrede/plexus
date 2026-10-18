#ifndef HPP_GUARD_PLEXUS_IO_COMPANION_EVENT_H
#define HPP_GUARD_PLEXUS_IO_COMPANION_EVENT_H

#include "plexus/wire/frame.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

// What one end of a co-host pair tells the other about the shared-memory lane of one topic: the publisher offers its
// ring, or does not, in its subscribe response; the subscriber answers that its receive companion is ready, and later,
// if it loses it, that it was withdrawn.
enum class companion_event : std::uint8_t
{
    offered,
    not_offered,
    ready,
    withdrawn,
};

// The subscriber's answers ride their own verbs; an offer rides the subscribe response instead.
inline std::optional<wire::msg_type> verb_of(companion_event event) noexcept
{
    if(event == companion_event::ready)
        return wire::msg_type::companion_ready;
    if(event == companion_event::withdrawn)
        return wire::msg_type::companion_withdrawn;
    return std::nullopt;
}

}

#endif
