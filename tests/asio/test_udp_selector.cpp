// The transport_selector two-axis composition, as a PURE unit test (no socket, no
// io_context). The selector composes locality (the tier) x reliability (the
// scheme-encoded delivery class) FROM THE ENDPOINT ALONE — the dial(ep) reality,
// where the scheme is the only routing discriminator the engine path carries. The
// matrix pins:
//   * the tier: "unix"/"inproc" -> local (locality wins regardless of the class);
//     "udp"/"udpr"/"tcp"/"tls" -> remote.
//   * the scheme->reliability class: "udp" -> best_effort, "udpr" ->
//     reliable_datagram, "tcp"/"tls" -> reliable, "unix"/"inproc" -> unspecified
//     (local; reliability moot).
//   * the value-object invariant: two independently constructed selectors give
//     identical results (no global atomic, no setter — there is no process state to
//     mutate). A route is forced ONLY by passing a different scheme, never by
//     mutating the selector.
// Crucially it pins reliability_of_scheme("udpr") == reliable_datagram and
// reliability_of_scheme("udp") == best_effort: the scheme-encode that makes the
// reliable-datagram opt-in reachable through dial(ep).

#include "plexus/io/endpoint.h"
#include "plexus/io/reliability.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/reliability_requirement.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

namespace pio = plexus::io;

TEST_CASE("udp selector: the tier classifies same-host local and everything else remote",
          "[udp][selector][tier]")
{
    pio::transport_selector sel;
    const auto any = pio::reliability_hint::unspecified;

    // Locality wins: a same-host scheme is local regardless of the reliability hint.
    REQUIRE(sel.select({"unix", "/tmp/s"}, any) == pio::transport_kind::local);
    REQUIRE(sel.select({"unix", "/tmp/s"}, pio::reliability_hint::reliable) == pio::transport_kind::local);
    REQUIRE(sel.select({"inproc", "node-a"}, any) == pio::transport_kind::local);

    // Every off-host scheme — including the two UDP spellings — is the remote tier.
    REQUIRE(sel.select({"udp", "127.0.0.1:5000"}, any) == pio::transport_kind::remote);
    REQUIRE(sel.select({"udpr", "127.0.0.1:5000"}, any) == pio::transport_kind::remote);
    REQUIRE(sel.select({"tcp", "127.0.0.1:5000"}, any) == pio::transport_kind::remote);
    REQUIRE(sel.select({"tls", "127.0.0.1:5000"}, any) == pio::transport_kind::remote);
}

TEST_CASE("udp selector: the scheme classifies its reliability class from the endpoint alone",
          "[udp][selector][reliability]")
{
    pio::transport_selector sel;

    // The load-bearing scheme-encode: udp is best_effort, udpr is the reliable-
    // datagram opt-in. Pinned by test so plan-05's later udpr retarget is visible.
    REQUIRE(sel.reliability_of_scheme("udp") == pio::reliability_hint::best_effort);
    REQUIRE(sel.reliability_of_scheme("udpr") == pio::reliability_hint::reliable_datagram);

    // The reliable-stream schemes classify reliable; same-host schemes are
    // unspecified (local; the reliability axis is moot once locality has won).
    REQUIRE(sel.reliability_of_scheme("tcp") == pio::reliability_hint::reliable);
    REQUIRE(sel.reliability_of_scheme("tls") == pio::reliability_hint::reliable);
    REQUIRE(sel.reliability_of_scheme("unix") == pio::reliability_hint::unspecified);
    REQUIRE(sel.reliability_of_scheme("inproc") == pio::reliability_hint::unspecified);

    // An unrecognized scheme classifies unspecified: it carries no reliability claim
    // of its own (the tier gate above already classifies it remote, fail-closed).
    REQUIRE(sel.reliability_of_scheme("ws") == pio::reliability_hint::unspecified);
}

TEST_CASE("udp selector: the selector is a pure value object — no state across calls or instances",
          "[udp][selector][value-object]")
{
    pio::transport_selector a;
    pio::transport_selector b;

    const pio::endpoint udpr{"udpr", "127.0.0.1:5000"};
    const pio::endpoint udp{"udp", "127.0.0.1:5000"};

    // Two independently constructed selectors agree, and a repeated call is stable —
    // there is no process state a test could (or should) mutate to force a route.
    REQUIRE(a.reliability_of_scheme(udpr.scheme) == b.reliability_of_scheme(udpr.scheme));
    REQUIRE(a.reliability_of_scheme(udp.scheme) == b.reliability_of_scheme(udp.scheme));
    REQUIRE(a.reliability_of_scheme("udpr") == pio::reliability_hint::reliable_datagram);
    REQUIRE(a.reliability_of_scheme("udpr") == a.reliability_of_scheme("udpr"));
    REQUIRE(a.select(udp, pio::reliability_hint::unspecified)
            == b.select(udp, pio::reliability_hint::unspecified));
}

TEST_CASE("udp selector: select consumes the hint without changing the tier",
          "[udp][selector][reliability]")
{
    pio::transport_selector sel;

    // The hint is tier-neutral: the locality tier is identical under every hint, so the
    // additive reliability consumption never regresses the classification.
    const pio::endpoint udp{"udp", "127.0.0.1:5000"};
    REQUIRE(sel.select(udp, pio::reliability_hint::reliable)
            == sel.select(udp, pio::reliability_hint::best_effort));
    REQUIRE(sel.select(udp, pio::reliability_hint::best_effort)
            == sel.select(udp, pio::reliability_hint::unspecified));

    const pio::endpoint unix_ep{"unix", "/tmp/s"};
    REQUIRE(sel.select(unix_ep, pio::reliability_hint::reliable)
            == sel.select(unix_ep, pio::reliability_hint::unspecified));
}

TEST_CASE("udp selector: reliability_class enforces the no-silent-downgrade rule",
          "[udp][selector][reliability]")
{
    pio::transport_selector sel;
    const auto reliable = pio::reliability_hint::reliable;
    const auto best_effort = pio::reliability_hint::best_effort;
    using verdict = pio::reliability_admissibility;

    // A reliable hint over a best_effort scheme (udp/dtls) is refused — never silently
    // downgraded to a lossy path.
    REQUIRE(sel.reliability_class({"udp", "h:1"}, reliable) == verdict::downgrade_refused);
    REQUIRE(sel.reliability_class({"dtls", "h:1"}, reliable) == verdict::downgrade_refused);

    // A reliable hint over a reliable scheme is admissible; udpr (reliable-datagram)
    // satisfies reliable — the no-silent-downgrade-to-bare-udp rule.
    REQUIRE(sel.reliability_class({"tcp", "h:1"}, reliable) == verdict::admissible);
    REQUIRE(sel.reliability_class({"tls", "h:1"}, reliable) == verdict::admissible);
    REQUIRE(sel.reliability_class({"udpr", "h:1"}, reliable) == verdict::admissible);
    REQUIRE(sel.reliability_class({"unix", "/tmp/s"}, reliable) == verdict::admissible);
    REQUIRE(sel.reliability_class({"inproc", "node"}, reliable) == verdict::admissible);

    // Fail-CLOSED: an unrecognized scheme is NOT admissible for a reliable hint.
    REQUIRE(sel.reliability_class({"ws", "h:1"}, reliable) == verdict::downgrade_refused);

    // A best_effort hint asks for no guarantee — admissible on any path.
    REQUIRE(sel.reliability_class({"udp", "h:1"}, best_effort) == verdict::admissible);
    REQUIRE(sel.reliability_class({"ws", "h:1"}, best_effort) == verdict::admissible);

    // The mirror invariant over EVERY scheme tested, unknown included: the selector
    // verdict and the engine gate are lock-step — a reliable hint is admissible iff
    // scheme_is_reliable. The two enforcement points cannot diverge for any scheme.
    for(std::string_view scheme : {"udp", "dtls", "tcp", "tls", "udpr", "unix", "inproc", "ws"})
    {
        const pio::endpoint ep{std::string{scheme}, "addr"};
        const bool admissible = sel.reliability_class(ep, reliable) == verdict::admissible;
        REQUIRE(admissible == pio::scheme_is_reliable(ep.scheme));
    }
}

TEST_CASE("udp selector: reliability_hint_of bridges the publisher's declared class",
          "[udp][selector][reliability]")
{
    pio::transport_selector sel;
    REQUIRE(sel.reliability_hint_of(pio::reliability::best_effort) == pio::reliability_hint::best_effort);
    REQUIRE(sel.reliability_hint_of(pio::reliability::reliable) == pio::reliability_hint::reliable);
}

TEST_CASE("udp selector: dispatch_class observably changes with the hint and leaves shm_eligible_for intact",
          "[udp][selector][dispatch]")
{
    pio::transport_selector sel;
    using pio::shm::dispatch_hint;

    // The declared dispatch hint observably steers the verdict: a set hint prefers the
    // fast path, none does not. For a LOCAL peer it coincides with shm_eligible_for.
    const pio::endpoint unix_ep{"unix", "/tmp/s"};
    REQUIRE(sel.dispatch_class(unix_ep, dispatch_hint::frequent));
    REQUIRE_FALSE(sel.dispatch_class(unix_ep, dispatch_hint::none));

    // For a REMOTE peer the verdict records the advisory preference (true iff any bit set).
    const pio::endpoint tcp_ep{"tcp", "127.0.0.1:5000"};
    REQUIRE(sel.dispatch_class(tcp_ep, dispatch_hint::frequent));
    REQUIRE_FALSE(sel.dispatch_class(tcp_ep, dispatch_hint::none));

    // shm_eligible_for is UNCHANGED: same-host AND a hint set -> true; a remote peer is
    // never shm-eligible even with a hint.
    REQUIRE(sel.shm_eligible_for(unix_ep, dispatch_hint::frequent));
    REQUIRE_FALSE(sel.shm_eligible_for(tcp_ep, dispatch_hint::frequent));
}

TEST_CASE("udp selector: the new verdicts preserve the pure value-object invariant",
          "[udp][selector][value-object]")
{
    pio::transport_selector a;
    pio::transport_selector b;
    using pio::shm::dispatch_hint;

    // Two independently constructed selectors agree on every new verdict; a verdict is
    // forced ONLY by a different (scheme, hint), never by mutating the selector.
    const pio::endpoint udp{"udp", "127.0.0.1:5000"};
    const pio::endpoint tcp{"tcp", "127.0.0.1:5000"};
    REQUIRE(a.reliability_class(udp, pio::reliability_hint::reliable)
            == b.reliability_class(udp, pio::reliability_hint::reliable));
    REQUIRE(a.reliability_class(tcp, pio::reliability_hint::reliable)
            == b.reliability_class(tcp, pio::reliability_hint::reliable));
    REQUIRE(a.dispatch_class({"unix", "/tmp/s"}, dispatch_hint::frequent)
            == b.dispatch_class({"unix", "/tmp/s"}, dispatch_hint::frequent));
    REQUIRE(a.reliability_hint_of(pio::reliability::reliable)
            == b.reliability_hint_of(pio::reliability::reliable));

    // A different scheme forces a different reliability verdict — the discriminator is the
    // input, not selector state.
    REQUIRE(a.reliability_class(udp, pio::reliability_hint::reliable)
            != a.reliability_class(tcp, pio::reliability_hint::reliable));
}
