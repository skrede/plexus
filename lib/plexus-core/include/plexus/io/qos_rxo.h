#ifndef HPP_GUARD_PLEXUS_IO_QOS_RXO_H
#define HPP_GUARD_PLEXUS_IO_QOS_RXO_H

#include "plexus/topic_qos.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/reliability.h"
#include "plexus/io/subscriber_qos.h"

#include <cstddef>
#include <cstdint>

namespace plexus::io {

// The Request-vs-Offered (RxO) compatibility relations: for each comparable QoS
// field the producer's OFFERED value must be at-least-as-strong as the subscriber's
// REQUESTED value. Each relation is a pure value comparison — no I/O, no registry
// dependency — so the matrix is auditable and unit-testable in isolation. The
// carry-only fields (priority, dispatch, congestion, history depth, reach) are NOT
// compared here: they ride along without ever gating a match.

// Reliability: a reliable request needs a reliable offer (the no-silent-downgrade
// direction). A best-effort request matches any offer.
inline bool reliability_compatible(reliability offered, bool requested_reliable) noexcept
{
    return !requested_reliable || offered == reliability::reliable;
}

// Durability on the none < latest < all ordering. A latching topic offers the
// retained-history class (all), a non-latching one offers none. A `none` request
// matches any offer; a `latest`/`all` request needs the latching offer.
inline bool durability_compatible(bool offered_latch, durability requested) noexcept
{
    return requested == durability::none || offered_latch;
}

// Deadline period: the offered period must be no slower than the requested one
// (offered <= requested). 0 on either side is "not offered / not requested" and is
// always compatible.
inline bool deadline_compatible(std::uint64_t offered_period, std::uint64_t requested_period) noexcept
{
    return offered_period == 0 || requested_period == 0 || offered_period <= requested_period;
}

// Lease (liveliness) period: the offered lease must be no longer than the requested
// one (offered <= requested). 0 on either side is "not offered / not requested" and
// is always compatible.
inline bool lease_compatible(std::uint64_t offered_lease, std::uint64_t requested_lease) noexcept
{
    return offered_lease == 0 || requested_lease == 0 || offered_lease <= requested_lease;
}

// Source identity: a subscriber that requires source identity needs a producer that
// offers it (a capability contract — you cannot fabricate an identity the producer
// never advertised). A non-requiring subscriber matches any offer.
inline bool source_identity_compatible(bool offered_emit, bool requires_sid) noexcept
{
    return !requires_sid || offered_emit;
}

// Per-message size: the publisher's OFFERED effective-max must fit the subscriber's
// REQUESTED ceiling (offered_effective_max <= requested_max) — a subscriber that
// declared a smaller ceiling cannot receive a larger message, so the incompatible
// direction is a publisher that can emit past what the subscriber accepts. A 0
// requested ceiling is "not requested" and is always compatible (the offered side is
// already resolved through effective_max at the call site, so its 0=unset is gone).
inline bool max_message_bytes_compatible(std::size_t offered_effective_max,
                                         std::uint32_t requested_max) noexcept
{
    return requested_max == 0 || offered_effective_max <= requested_max;
}

// The soft-field bit allocation for the degraded-field bitmask. Each bit names a
// soft (consumer-tunable) field that went unsatisfied under a permissive accept.
// Source identity is NOT a degradable field — it is the one always-hard verdict.
constexpr std::uint8_t k_rxo_field_reliability       = 0x01;
constexpr std::uint8_t k_rxo_field_durability        = 0x02;
constexpr std::uint8_t k_rxo_field_deadline          = 0x04;
constexpr std::uint8_t k_rxo_field_lease             = 0x08;
constexpr std::uint8_t k_rxo_field_max_message_bytes = 0x10;

// The verdict of a full RxO check. `compatible` admits cleanly; `degraded` admits a
// permissive subscriber but with one or more soft fields unsatisfied (the surfaced
// bitmask); `incompatible_qos` refuses a strict subscriber on a soft field with the
// failing-field bitmask as the reason; `source_identity_incompatible` refuses the one
// always-hard field regardless of the subscriber's chosen mode.
enum class rxo_verdict : std::uint8_t
{
    compatible,
    degraded,
    incompatible_qos,
    source_identity_incompatible
};

// The result of rxo_check: the verdict plus the bitmask of soft fields that went
// unsatisfied (the reason on a strict refusal, the surfaced set on a permissive
// degraded-accept). degraded_fields is 0 on a clean compatible verdict and on the
// always-hard source-identity refusal (which is not a degradable field).
struct rxo_result
{
    rxo_verdict   verdict;
    std::uint8_t  degraded_fields = 0;
};

// Compose the per-field relations into a single verdict. Source identity is the one
// always-hard field: it is evaluated FIRST and refuses regardless of the subscriber's
// chosen rxo_mode. The soft fields (reliability, durability, deadline, lease, max
// message bytes) are mode-gated: a strict subscriber is refused with the failing-field
// bitmask as the reason; a permissive subscriber connects but the unsatisfied set is
// surfaced so the accept is never silent. global_default resolves the offered topic's
// 0=unset max into the publisher's effective-max for the size relation.
inline rxo_result rxo_check(const topic_qos &offered, bool offers_source_identity,
                            std::size_t global_default,
                            const subscriber_qos &requested) noexcept
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
    if(!max_message_bytes_compatible(effective_max(offered, global_default),
                                     requested.requested_max_message_bytes))
        bits |= k_rxo_field_max_message_bytes;

    if(bits == 0)
        return {rxo_verdict::compatible, 0};
    if(requested.rxo == rxo_mode::strict)
        return {rxo_verdict::incompatible_qos, bits};
    return {rxo_verdict::degraded, bits};
}

}

#endif
