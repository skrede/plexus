#ifndef HPP_GUARD_PLEXUS_IO_RELIABILITY_REQUIREMENT_H
#define HPP_GUARD_PLEXUS_IO_RELIABILITY_REQUIREMENT_H

#include <cstdint>
#include <string_view>

namespace plexus::io {

// A subscription's per-demand delivery-guarantee requirement — the demand-side knob
// the routing_engine's reliability gate enforces. Mirrors the locality reach_mask
// shape: a required-with-default param whose default value (`any`) is permissive
// (anything connects; mechanism-not-policy — CONTEXT LOCKED), and whose strict value
// (`reliable`) refuses a demand the target peer's transport cannot meet.
//   * any      — no requirement: the subscription connects regardless of the peer's
//                transport reliability (the permissive default; existing callers are
//                never refused).
//   * reliable — strict opt-in: the subscription requires a reliable transport, and a
//                demand toward a best_effort (or unknown) peer is refused PRE-DIAL.
// This is NOT the io::reliability choice enum (best_effort/reliable, the topic's own
// class): it is the SUBSCRIBER's requirement on the path, with a permissive `any`.
enum class reliability_requirement : std::uint8_t
{
    any,
    reliable,
};

// Does an endpoint scheme satisfy a reliable requirement? This mapping is the engine-
// side mirror of the asio selector's reliability_of_scheme and MUST stay consistent
// with it: plain "udp" is best_effort (does NOT satisfy reliable); "udpr" (the
// reliable-datagram opt-in), "tcp", "tls" are reliable, and "unix"/"inproc" are
// lossless local streams (reliable). An unrecognized scheme is treated as NOT reliable
// (fail-closed: never admit a strict-reliable demand over a transport we cannot prove
// is reliable).
inline bool scheme_is_reliable(std::string_view scheme) noexcept
{
    return scheme == "udpr" || scheme == "tcp" || scheme == "tls" || scheme == "unix" ||
            scheme == "inproc"; // plain "udp" and unknowns: not reliable
}

}

#endif
