#ifndef HPP_GUARD_PLEXUS_IO_QOS_RXO_H
#define HPP_GUARD_PLEXUS_IO_QOS_RXO_H

#include "plexus/topic_qos.h"

#include "plexus/io/reliability.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/subscriber_qos.h"

#include <cstddef>
#include <cstdint>

namespace plexus::io {

// The Request-vs-Offered (RxO) compatibility relations: for each comparable QoS field the
// producer's OFFERED value must be at-least-as-strong as the subscriber's REQUESTED value.

inline bool reliability_compatible(reliability offered, bool requested_reliable) noexcept
{
    return !requested_reliable || offered == reliability::reliable;
}

inline bool durability_compatible(bool offered_latch, durability requested) noexcept
{
    return requested == durability::none || offered_latch;
}

// 0 on either side is "not offered / not requested" and is always compatible.
inline bool deadline_compatible(std::uint64_t offered_period, std::uint64_t requested_period) noexcept
{
    return offered_period == 0 || requested_period == 0 || offered_period <= requested_period;
}

inline bool lease_compatible(std::uint64_t offered_lease, std::uint64_t requested_lease) noexcept
{
    return offered_lease == 0 || requested_lease == 0 || offered_lease <= requested_lease;
}

inline bool source_identity_compatible(bool offered_emit, bool requires_sid) noexcept
{
    return !requires_sid || offered_emit;
}

inline bool max_message_bytes_compatible(std::size_t offered_effective_max, std::uint32_t requested_max) noexcept
{
    return requested_max == 0 || offered_effective_max <= requested_max;
}

constexpr std::uint8_t k_rxo_field_reliability       = 0x01;
constexpr std::uint8_t k_rxo_field_durability        = 0x02;
constexpr std::uint8_t k_rxo_field_deadline          = 0x04;
constexpr std::uint8_t k_rxo_field_lease             = 0x08;
constexpr std::uint8_t k_rxo_field_max_message_bytes = 0x10;

enum class rxo_verdict : std::uint8_t
{
    compatible,
    degraded,
    incompatible_qos,
    source_identity_incompatible
};

struct rxo_result
{
    rxo_verdict verdict;
    std::uint8_t degraded_fields = 0;
};

// Source identity is the one always-hard field: it is evaluated FIRST and refuses regardless of
// the subscriber's rxo_mode. The soft fields are mode-gated.
inline rxo_result rxo_check(const topic_qos &offered, bool offers_source_identity, std::size_t global_default, const subscriber_qos &requested) noexcept
{
    if(!source_identity_compatible(offers_source_identity, requested.requires_source_identity))
        return {rxo_verdict::source_identity_incompatible, 0};

    std::uint8_t bits = 0;
    if(!reliability_compatible(offered.reliability, requested.requested_reliability_reliable))
        bits |= k_rxo_field_reliability;
    if(!durability_compatible(offered.latch, requested.durability_mode))
        bits |= k_rxo_field_durability;
    if(!deadline_compatible(offered.offered_deadline_ns, requested.requested_deadline_ns))
        bits |= k_rxo_field_deadline;
    if(!lease_compatible(offered.offered_lease_ns, requested.requested_lease_ns))
        bits |= k_rxo_field_lease;
    if(!max_message_bytes_compatible(effective_max(offered, global_default), requested.requested_max_message_bytes))
        bits |= k_rxo_field_max_message_bytes;

    if(bits == 0)
        return {rxo_verdict::compatible, 0};
    if(requested.rxo == rxo_mode::strict)
        return {rxo_verdict::incompatible_qos, bits};
    return {rxo_verdict::degraded, bits};
}

}

#endif
