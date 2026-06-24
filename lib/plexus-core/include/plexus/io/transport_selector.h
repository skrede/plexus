#ifndef HPP_GUARD_PLEXUS_IO_TRANSPORT_SELECTOR_H
#define HPP_GUARD_PLEXUS_IO_TRANSPORT_SELECTOR_H

#include "plexus/io/endpoint.h"
#include "plexus/io/reliability.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/io/reliability_requirement.h"

#include <cstdint>
#include <string_view>

namespace plexus::io {

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

// The verdict of composing a topic's reliability requirement with a path's own
// delivery class: a requirement is admissible on a path iff the path does NOT
// silently downgrade it. A reliable topic toward a best_effort (or unknown) path
// is downgrade_refused — the no-silent-downgrade rule, generalized from the
// reliable_datagram case so a reliable guarantee is never quietly weakened to a
// lossy one. A best_effort topic asks for no guarantee, so it is admissible on
// any path.
enum class reliability_admissibility : std::uint8_t
{
    admissible,
    downgrade_refused,
};

// A small value object owned by the multiplexing transport. It holds NO transport
// handles and NO hint map — it is a pure function of the endpoint scheme (plus the
// reserved reliability axis). Same-host detection reads ep.scheme: "unix"/"inproc"/"shm"
// are same-host (local); everything else, INCLUDING "tcp", "tls", "udp", "dtls", and an
// unrecognized scheme, classifies remote — the most-restrictive-to-leak default, so a
// same-host-confined peer is never reached over an unknown transport. "tls" and "dtls"
// are remote members: they ride a network path, so locality confinement still excludes
// them. "shm" is the same-host shared-memory medium: it rides no wire at all, so it is
// unconditionally local — the locality confinement it falls under is the tightest there is.
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
    // The locality tier of an endpoint: a pure scheme classifier. The hint stays
    // TIER-NEUTRAL — locality always wins, so a same-host scheme is local under any
    // hint and an off-host scheme is remote under any hint. The reliability axis is
    // consumed SEPARATELY by reliability_class (the verdict that composes the hint
    // with the path's own class); the hint reaches select() only so the signature
    // stays stable for callers that thread the axis through dial(ep).
    transport_kind select(const endpoint &ep, reliability_hint /*tier-neutral; composed by reliability_class*/) const noexcept
    {
        if(ep.scheme == "unix" || ep.scheme == "inproc" || ep.scheme == "shm")
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
    reliability_hint reliability_of_scheme(std::string_view scheme) const noexcept
    {
        if(scheme == "udp" || scheme == "dtls")
            return reliability_hint::best_effort;
        if(scheme == "udpr")
            return reliability_hint::reliable_datagram;
        if(scheme == "tcp" || scheme == "tls")
            return reliability_hint::reliable;
        return reliability_hint::unspecified; // unix/inproc (local) and unknown: no claim
    }

    // Compose a topic's reliability requirement (expressed as a hint) with a path's
    // own delivery class into the no-silent-downgrade verdict. A reliable hint is
    // admissible ONLY on a scheme that scheme_is_reliable proves reliable (tcp/tls/
    // udpr/unix/inproc); a reliable hint toward a best_effort scheme (udp/dtls) OR an
    // unrecognized scheme is downgrade_refused — fail-CLOSED on unknown, never quietly
    // weakening a reliable guarantee. A best_effort/unspecified hint asks for no
    // guarantee, so it is admissible on any path. Driving the reliable branch off
    // scheme_is_reliable DIRECTLY (not off reliability_of_scheme) keeps this verdict
    // lock-step with the engine-side reliability gate for EVERY scheme, unknown
    // included — the documented mirror invariant the two enforcement points share.
    reliability_admissibility reliability_class(const endpoint &ep, reliability_hint hint) const noexcept
    {
        if(hint == reliability_hint::reliable && !scheme_is_reliable(ep.scheme))
            return reliability_admissibility::downgrade_refused;
        return reliability_admissibility::admissible;
    }

    // Bridge the PUBLISHER's declared topic_qos.reliability {best_effort, reliable}
    // into the selector's hint axis, so the declared class reaches reliability_class
    // as a real input rather than a hardcoded constant.
    reliability_hint reliability_hint_of(reliability r) const noexcept
    {
        return r == reliability::reliable ? reliability_hint::reliable : reliability_hint::best_effort;
    }

    // Does a topic's dispatch hint prefer the fast path? A transport-class preference: true iff
    // any hint bit is set. Tier-independent in value; the locality tier governs what it MEANS —
    // actionable for a local peer, advisory for a remote one (no remote fast member exists today).
    bool dispatch_class(const endpoint & /*tier governs meaning, not value*/, dispatch_hint h) const noexcept
    {
        return any_set(h);
    }

    // Prefer the fast local medium for a (peer, topic): same-host (local tier) AND any dispatch-hint
    // bit set — both necessary. A pure value-object verdict (no ring, no broker, no acquired-set); it
    // only answers "attempt the fast-local acquire?". The acquire + any wire fallback is the registry's.
    bool local_fast_eligible_for(const endpoint &ep, dispatch_hint h) const noexcept
    {
        return select(ep, reliability_hint::unspecified) == transport_kind::local && any_set(h);
    }
};

}

#endif
