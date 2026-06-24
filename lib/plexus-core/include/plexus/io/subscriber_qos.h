#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBER_QOS_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBER_QOS_H

#include "plexus/io/dispatch_hint.h"

#include <cstdint>

namespace plexus::io {

// Wire bytes {0,1,2} pin each enumerator (append-only, never renumbered).
enum class durability : std::uint8_t
{
    none   = 0,
    latest = 1,
    all    = 2
};

enum class delivery : std::uint8_t
{
    push = 0,
    pull = 1
};

enum class rxo_mode : std::uint8_t
{
    permissive = 0,
    strict     = 1
};

enum class attach_posture : std::uint8_t
{
    lenient = 0,
    strict  = 1
};

// The subscriber-CHOICE QoS, a distinct actor's decision from the publisher-declared topic_qos.
// Each 0-sentinel / enum default is a genuine "not requested" state (replay_depth=0 means "use
// the ring depth" for an `all` replay), never a euphemism for an unimplemented feature.
struct subscriber_qos
{
    durability durability_mode = durability::none;
    delivery delivery_mode     = delivery::push;
    std::uint32_t replay_depth = 0;

    bool requires_source_identity       = false;
    bool requested_reliability_reliable = false;
    std::uint64_t requested_deadline_ns = 0;
    std::uint64_t requested_lease_ns    = 0;
    std::uint8_t requested_priority     = 0;

    std::uint32_t requested_max_message_bytes = 0;

    rxo_mode rxo           = rxo_mode::permissive;
    attach_posture posture = attach_posture::lenient;

    // Local-only: NEVER encoded into the subscribe wire region (a cross-process demand leak is
    // forbidden). Set implicitly from the callback arity, overridable on a 3-arg subscriber.
    bool wants_message_info = true;

    // Local-only, never encoded: the upgrade converges on local state with no wire exchange. A
    // same-host subscriber with any bit set rescues itself from a hint-less publisher (the
    // bilateral OR — EITHER end's hint upgrades the pair).
    dispatch_hint dispatch = dispatch_hint::none;

    friend bool operator==(const subscriber_qos &, const subscriber_qos &) = default;
};

}

#endif
