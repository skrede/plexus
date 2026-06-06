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

#include "plexus/asio/mux_selector.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

TEST_CASE("udp selector: the tier classifies same-host local and everything else remote",
          "[udp][selector][tier]")
{
    pasio::transport_selector sel;
    const auto any = pasio::reliability_hint::unspecified;

    // Locality wins: a same-host scheme is local regardless of the reliability hint.
    REQUIRE(sel.select({"unix", "/tmp/s"}, any) == pasio::transport_kind::local);
    REQUIRE(sel.select({"unix", "/tmp/s"}, pasio::reliability_hint::reliable) == pasio::transport_kind::local);
    REQUIRE(sel.select({"inproc", "node-a"}, any) == pasio::transport_kind::local);

    // Every off-host scheme — including the two UDP spellings — is the remote tier.
    REQUIRE(sel.select({"udp", "127.0.0.1:5000"}, any) == pasio::transport_kind::remote);
    REQUIRE(sel.select({"udpr", "127.0.0.1:5000"}, any) == pasio::transport_kind::remote);
    REQUIRE(sel.select({"tcp", "127.0.0.1:5000"}, any) == pasio::transport_kind::remote);
    REQUIRE(sel.select({"tls", "127.0.0.1:5000"}, any) == pasio::transport_kind::remote);
}

TEST_CASE("udp selector: the scheme classifies its reliability class from the endpoint alone",
          "[udp][selector][reliability]")
{
    pasio::transport_selector sel;

    // The load-bearing scheme-encode: udp is best_effort, udpr is the reliable-
    // datagram opt-in. Pinned by test so plan-05's later udpr retarget is visible.
    REQUIRE(sel.reliability_of_scheme("udp") == pasio::reliability_hint::best_effort);
    REQUIRE(sel.reliability_of_scheme("udpr") == pasio::reliability_hint::reliable_datagram);

    // The reliable-stream schemes classify reliable; same-host schemes are
    // unspecified (local; the reliability axis is moot once locality has won).
    REQUIRE(sel.reliability_of_scheme("tcp") == pasio::reliability_hint::reliable);
    REQUIRE(sel.reliability_of_scheme("tls") == pasio::reliability_hint::reliable);
    REQUIRE(sel.reliability_of_scheme("unix") == pasio::reliability_hint::unspecified);
    REQUIRE(sel.reliability_of_scheme("inproc") == pasio::reliability_hint::unspecified);

    // An unrecognized scheme classifies unspecified: it carries no reliability claim
    // of its own (the tier gate above already classifies it remote, fail-closed).
    REQUIRE(sel.reliability_of_scheme("ws") == pasio::reliability_hint::unspecified);
}

TEST_CASE("udp selector: the selector is a pure value object — no state across calls or instances",
          "[udp][selector][value-object]")
{
    pasio::transport_selector a;
    pasio::transport_selector b;

    const pio::endpoint udpr{"udpr", "127.0.0.1:5000"};
    const pio::endpoint udp{"udp", "127.0.0.1:5000"};

    // Two independently constructed selectors agree, and a repeated call is stable —
    // there is no process state a test could (or should) mutate to force a route.
    REQUIRE(a.reliability_of_scheme(udpr.scheme) == b.reliability_of_scheme(udpr.scheme));
    REQUIRE(a.reliability_of_scheme(udp.scheme) == b.reliability_of_scheme(udp.scheme));
    REQUIRE(a.reliability_of_scheme("udpr") == pasio::reliability_hint::reliable_datagram);
    REQUIRE(a.reliability_of_scheme("udpr") == a.reliability_of_scheme("udpr"));
    REQUIRE(a.select(udp, pasio::reliability_hint::unspecified)
            == b.select(udp, pasio::reliability_hint::unspecified));
}
