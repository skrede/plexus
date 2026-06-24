#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_EVENT_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_EVENT_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

enum class security_kind : std::uint8_t
{
    unauthorized_attach,
    downgrade_refused,
    posture_mismatch,
    stream_tamper_teardown,
    rekey
};

enum class security_cause : std::uint8_t
{
    none,
    tag_verify_failed,
    unknown_key_epoch,
    malformed_frame
};

struct security_event
{
    security_kind kind;
    node_id peer;
    security_cause cause; // meaningful only on stream_tamper_teardown
};

}

#endif
