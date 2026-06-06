#ifndef HPP_GUARD_PLEXUS_ASIO_MUX_SELECTOR_H
#define HPP_GUARD_PLEXUS_ASIO_MUX_SELECTOR_H

#include "plexus/io/endpoint.h"

#include <cstdint>
#include <string_view>

namespace plexus::asio {

// The delivery-guarantee axis the selector composes with the locality tier. The
// enumerators are now live (a lossy datagram backend exists): a scheme classifies
// into ONE of these classes.
//   * unspecified  — the scheme makes no reliability claim of its own (same-host
//     "unix"/"inproc", where locality wins and the axis is moot; or an unrecognized
//     scheme that carries no claim).
//   * best_effort  — lossy, fire-and-forget datagrams (the "udp" scheme).
//   * reliable     — a lossless ordered stream (the "tcp"/"tls" schemes).
//   * reliable_datagram — the datagram-with-retransmit opt-in (the "udpr" scheme):
//     a reliable guarantee over a datagram substrate. Until its data ARQ engine
//     exists, the multiplexing transport routes it to the reliable STREAM member
//     (never to bare best_effort UDP — that would silently downgrade the guarantee).
// select() still TAKES a hint param (unchanged signature); the mux derives the hint
// from ep.scheme via reliability_of_scheme so the axis reaches dial(ep).
enum class reliability_hint : std::uint8_t
{
    unspecified = 0,
    best_effort,
    reliable,
    reliable_datagram,
};

// The tag the multiplexing transport maps to one of its member transports: a same-host
// peer is reached over the local stream, an off-host peer over a network stream. The
// remote tier holds the plaintext "tcp"/"udp" members AND the secure "tls"/"dtls" members
// — the tier classifies locality, not the wire protocol, so "tls" and "dtls" stay remote
// (and the same-host locality confinement still excludes them: a host-confined peer is
// never reached over tls or dtls even though they encrypt). The tls-vs-tcp and dtls-vs-udp
// discrimination is the multiplexing transport's job (exact "tls"/"dtls" scheme branches
// within the remote tier), not the selector's — the tier enum stays local/remote.
enum class transport_kind : std::uint8_t
{
    local,
    remote,
};

// A small value object owned by the multiplexing transport. It holds NO transport
// handles and NO hint map — it is a pure function of the endpoint scheme (plus the
// reserved reliability axis). Same-host detection reads ep.scheme: "unix"/"inproc" are
// same-host (local); everything else, INCLUDING "tcp", "tls", "udp", "dtls", and an
// unrecognized scheme, classifies remote — the most-restrictive-to-leak default, so a
// same-host-confined peer is never reached over an unknown transport. "tls" and "dtls"
// are remote members: they ride a network path, so locality confinement still excludes them.
//
// transport_kind::local names the same-host TIER, not a concrete transport. The current
// multiplexing transport maps that tier to its only same-host member, AF_UNIX — so an
// "inproc" endpoint (also same-host) classifies local but has no member to bind/dial it
// through this mux. An "inproc" endpoint must therefore not be dialed through this mux
// until it owns an in-process member; the classification is correct for the tier, the
// routing is bounded by the member set the mux composes.
class transport_selector
{
public:
    [[nodiscard]] transport_kind select(const io::endpoint &ep,
                                        reliability_hint /*reserved for caller composition*/) const noexcept
    {
        if(ep.scheme == "unix" || ep.scheme == "inproc")
            return transport_kind::local;
        return transport_kind::remote;
    }

    // Classify an endpoint scheme into its reliability class — the value-object
    // expression of the (locality-tier, scheme->reliability) composition that is
    // REACHABLE through dial(ep), where the scheme is the only routing discriminator
    // the engine path carries. The mux feeds ep.scheme through here instead of
    // hardcoding a reserved hint, so the reliable-datagram opt-in is engine-reachable.
    //
    // The two UDP spellings are distinct schemes: plain "udp" is best_effort (lossy);
    // "udpr" is the reliable-datagram opt-in ("udp, reliable" — the shortest plexus-
    // native spelling; "udp+arq" was considered and rejected as noisier on the wire-
    // scheme). "dtls" is secure-best_effort (encrypted unreliable datagrams, the secure
    // parallel of "udp") — so it classifies best_effort here for documentation honesty,
    // but the mux routes "dtls" by an EXPLICIT scheme branch (like "tls"), not by this
    // reliability axis; this arm is documentation-only and never reached on the dtls path.
    // Same-host schemes classify unspecified: locality has already won, so the reliability
    // axis is moot. An unrecognized scheme also classifies unspecified — it carries no
    // reliability claim (the tier gate classifies it remote, fail-closed). This mapping is
    // the contract the routing_engine reliability gate mirrors.
    [[nodiscard]] reliability_hint reliability_of_scheme(std::string_view scheme) const noexcept
    {
        if(scheme == "udp" || scheme == "dtls")
            return reliability_hint::best_effort;
        if(scheme == "udpr")
            return reliability_hint::reliable_datagram;
        if(scheme == "tcp" || scheme == "tls")
            return reliability_hint::reliable;
        return reliability_hint::unspecified;   // unix/inproc (local) and unknown: no claim
    }
};

}

#endif
