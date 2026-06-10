#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_EVENT_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_EVENT_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

// Which security transition a fired event reports. unauthorized_attach is an attach
// the admission gate refused; downgrade_refused is a tampered cipher/version offer
// caught by the transcript binding; posture_mismatch is a secured-vs-plain refusal in
// either direction; stream_tamper_teardown is a failed AEAD verification on an ordered
// stream (which terminates the session); rekey marks a live-channel key rotation.
enum class security_kind : std::uint8_t
{
    unauthorized_attach,
    downgrade_refused,
    posture_mismatch,
    stream_tamper_teardown,
    rekey
};

// Why a stream tamper teardown fired, carried on the stream_tamper_teardown kind. It
// is a small code, never a heap string on the teardown path: the human trail is the
// rate-limited logger warn, this is the machine-readable cause an observer reads.
enum class security_cause : std::uint8_t
{
    none,
    tag_verify_failed,
    unknown_key_epoch,
    malformed_frame
};

// The observable a security transition carries up the dedicated settable callback the
// engine routes to its observer seam. Like liveness_event it is a flat POD — NOT a
// serialization artifact and NOT a lifecycle edge (an unauthorized attach ALSO rides
// the lifecycle rejected edge; this is the distinct security-surface fan-out). peer is
// the pinned session peer the transition concerns; cause is meaningful only on
// stream_tamper_teardown. Datagram replay/tamper drops are COUNTED, never fired here.
struct security_event
{
    security_kind  kind;
    node_id        peer;
    security_cause cause;
};

}

#endif
