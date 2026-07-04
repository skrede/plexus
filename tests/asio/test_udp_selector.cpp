#include "test_udp_selector_common.h"

TEST_CASE("udp selector: the tier classifies same-host local and everything else remote", "[udp][selector][tier]")
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

TEST_CASE("udp selector: the scheme classifies its reliability class from the endpoint alone", "[udp][selector][reliability]")
{
    pio::transport_selector sel;

    // The load-bearing scheme-encode: udp is best_effort, udpr is the reliable-
    // datagram opt-in. Pinned by test so a later udpr retarget stays visible.
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

TEST_CASE("udp selector: the selector is a pure value object - no state across calls or instances", "[udp][selector][value-object]")
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
    REQUIRE(a.select(udp, pio::reliability_hint::unspecified) == b.select(udp, pio::reliability_hint::unspecified));
}

TEST_CASE("udp selector: select consumes the hint without changing the tier", "[udp][selector][reliability]")
{
    pio::transport_selector sel;

    // The hint is tier-neutral: the locality tier is identical under every hint, so the
    // additive reliability consumption never regresses the classification.
    const pio::endpoint udp{"udp", "127.0.0.1:5000"};
    REQUIRE(sel.select(udp, pio::reliability_hint::reliable) == sel.select(udp, pio::reliability_hint::best_effort));
    REQUIRE(sel.select(udp, pio::reliability_hint::best_effort) == sel.select(udp, pio::reliability_hint::unspecified));

    const pio::endpoint unix_ep{"unix", "/tmp/s"};
    REQUIRE(sel.select(unix_ep, pio::reliability_hint::reliable) == sel.select(unix_ep, pio::reliability_hint::unspecified));
}

TEST_CASE("udp selector: reliability_class enforces the no-silent-downgrade rule", "[udp][selector][reliability]")
{
    pio::transport_selector sel;
    const auto reliable    = pio::reliability_hint::reliable;
    const auto best_effort = pio::reliability_hint::best_effort;
    using verdict          = pio::reliability_admissibility;

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
