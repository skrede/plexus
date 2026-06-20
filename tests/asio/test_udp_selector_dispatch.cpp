#include "test_udp_selector_common.h"

TEST_CASE("udp selector: reliability_hint_of bridges the publisher's declared class",
          "[udp][selector][reliability]")
{
    pio::transport_selector sel;
    REQUIRE(sel.reliability_hint_of(pio::reliability::best_effort) ==
            pio::reliability_hint::best_effort);
    REQUIRE(sel.reliability_hint_of(pio::reliability::reliable) == pio::reliability_hint::reliable);
}

TEST_CASE("udp selector: dispatch_class observably changes with the hint and leaves "
          "shm_eligible_for intact",
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
    REQUIRE(a.reliability_class(udp, pio::reliability_hint::reliable) ==
            b.reliability_class(udp, pio::reliability_hint::reliable));
    REQUIRE(a.reliability_class(tcp, pio::reliability_hint::reliable) ==
            b.reliability_class(tcp, pio::reliability_hint::reliable));
    REQUIRE(a.dispatch_class({"unix", "/tmp/s"}, dispatch_hint::frequent) ==
            b.dispatch_class({"unix", "/tmp/s"}, dispatch_hint::frequent));
    REQUIRE(a.reliability_hint_of(pio::reliability::reliable) ==
            b.reliability_hint_of(pio::reliability::reliable));

    // A different scheme forces a different reliability verdict — the discriminator is the
    // input, not selector state.
    REQUIRE(a.reliability_class(udp, pio::reliability_hint::reliable) !=
            a.reliability_class(tcp, pio::reliability_hint::reliable));
}
