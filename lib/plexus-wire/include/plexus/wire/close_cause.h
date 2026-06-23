#ifndef HPP_GUARD_PLEXUS_WIRE_CLOSE_CAUSE_H
#define HPP_GUARD_PLEXUS_WIRE_CLOSE_CAUSE_H

#include "plexus/wire/frame_reassembler.h"

#include <cstdint>

namespace plexus::wire {

// The protocol-close vocabulary every byte_channel reports through on_protocol_close,
// distinct from a transport (network) error: a close signal carries only actionable
// reasons — the three framing violations, the no-progress timeout, and the one non-fatal
// serial CRC mismatch — never feed_error::none nor the dead no_progress enumerator (the
// stream no-progress timer keys off a size-proportional deadline, not a reassembler signal).
enum class close_cause : std::uint8_t
{
    invalid_magic,
    payload_too_large,
    buffer_overflow,
    no_progress_timeout,
    // Append-only; raised ONLY by the serial CRC decorator. The ONE non-fatal cause
    // (drop+resync), surfaced through a SEPARATE on_frame_dropped seam, never on_protocol_close.
    crc_mismatch
};

inline close_cause to_close_cause(feed_error error)
{
    switch(error)
    {
        case feed_error::payload_too_large: return close_cause::payload_too_large;
        case feed_error::buffer_overflow:   return close_cause::buffer_overflow;
        default:                            return close_cause::invalid_magic;
    }
}

}

#endif
