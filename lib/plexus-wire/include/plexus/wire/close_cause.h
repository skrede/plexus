#ifndef HPP_GUARD_PLEXUS_WIRE_CLOSE_CAUSE_H
#define HPP_GUARD_PLEXUS_WIRE_CLOSE_CAUSE_H

#include "plexus/wire/frame_reassembler.h"

#include <cstdint>

namespace plexus::wire {

enum class close_cause : std::uint8_t
{
    invalid_magic,
    payload_too_large,
    buffer_overflow,
    no_progress_timeout,
    // Append-only. The one non-fatal cause (drop+resync), raised only by the serial CRC
    // decorator and surfaced through on_frame_dropped, never on_protocol_close.
    crc_mismatch
};

inline close_cause to_close_cause(feed_error error)
{
    switch(error)
    {
        case feed_error::payload_too_large:
            return close_cause::payload_too_large;
        case feed_error::buffer_overflow:
            return close_cause::buffer_overflow;
        default:
            return close_cause::invalid_magic;
    }
}

}

#endif
