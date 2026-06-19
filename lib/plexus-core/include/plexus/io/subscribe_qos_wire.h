#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBE_QOS_WIRE_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBE_QOS_WIRE_H

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/wire/subscribe.h"

#include <cstdint>

namespace plexus::io {

// The wire layer carries no core dependency, so the core<->wire QoS lift lives here.
inline wire::subscribe_qos_region to_wire_region(const subscriber_qos &q)
{
    std::uint8_t flags = 0;
    if(q.requires_source_identity)
        flags |= wire::detail::k_qos_flag_requires_source_identity;
    if(q.requested_reliability_reliable)
        flags |= wire::detail::k_qos_flag_requested_reliable;
    if(q.rxo == rxo_mode::strict)
        flags |= wire::detail::k_qos_flag_rxo_strict;
    if(q.posture == attach_posture::strict)
        flags |= wire::detail::k_qos_flag_typed_strict;
    return wire::subscribe_qos_region{.durability    = static_cast<std::uint8_t>(q.durability_mode),
                                      .delivery_mode = static_cast<std::uint8_t>(q.delivery_mode),
                                      .replay_depth  = q.replay_depth,
                                      .requested_flags             = flags,
                                      .requested_deadline_ns       = q.requested_deadline_ns,
                                      .requested_lease_ns          = q.requested_lease_ns,
                                      .requested_priority          = q.requested_priority,
                                      .requested_max_message_bytes = q.requested_max_message_bytes};
}

inline subscriber_qos from_wire_region(const wire::subscribe_qos_region &r)
{
    return subscriber_qos{
            .durability_mode = static_cast<durability>(r.durability),
            .delivery_mode   = static_cast<delivery>(r.delivery_mode),
            .replay_depth    = r.replay_depth,
            .requires_source_identity =
                    (r.requested_flags & wire::detail::k_qos_flag_requires_source_identity) != 0,
            .requested_reliability_reliable =
                    (r.requested_flags & wire::detail::k_qos_flag_requested_reliable) != 0,
            .requested_deadline_ns       = r.requested_deadline_ns,
            .requested_lease_ns          = r.requested_lease_ns,
            .requested_priority          = r.requested_priority,
            .requested_max_message_bytes = r.requested_max_message_bytes,
            .rxo     = (r.requested_flags & wire::detail::k_qos_flag_rxo_strict) != 0
                    ? rxo_mode::strict
                    : rxo_mode::permissive,
            .posture = (r.requested_flags & wire::detail::k_qos_flag_typed_strict) != 0
                    ? attach_posture::strict
                    : attach_posture::lenient};
}

}

#endif
