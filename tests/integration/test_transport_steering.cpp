// The QOS steering gate: a publisher's declared topic_qos.reliability and dispatch
// OBSERVABLY change the transport-class verdict at the pure value-object selector — the
// honest enforcement of "the declared field steers the class decision" given the dial(ep)
// reality, where the scheme is the only routing discriminator the engine path carries.
//
// reliability (QOS-08): a reliable topic toward a best_effort path is downgrade_refused
// while toward a reliable path it is admissible, and a best_effort topic is permissive
// everywhere — the no-silent-downgrade rule. The publisher's declared class reaches the
// verdict through reliability_hint_of, so this is a behavioral gate, not a serialization
// test. The selector verdict and the engine-side scheme_is_reliable gate are asserted
// CONSISTENT for every scheme (unknown included) so the two enforcement points cannot
// diverge.
//
// dispatch (QOS-06): a dispatch-hinted topic observably changes dispatch_class vs a
// hintless one; for a remote peer the verdict is advisory (the current mux has no
// datagram member to switch to). shm_eligible_for is preserved beside the generalized
// verdict.
//
// Each discriminating assertion is looped to pin reproducibility (the standing rule:
// never declare a transport/selection property from a single run, even for a
// deterministic pure function). The subscriber-side reliability gate is covered by the
// seam reliability-enforcement test; this file stays selector-class-focused.

#include "plexus/io/endpoint.h"
#include "plexus/io/reliability.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/reliability_requirement.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

namespace pio = plexus::io;

namespace {

constexpr int k_reps = 100;

}

TEST_CASE("transport steering: a declared reliable topic is refused on a best_effort path and "
          "admissible on a reliable one",
          "[steering][reliability]")
{
    pio::transport_selector sel;
    using verdict = pio::reliability_admissibility;

    // The publisher declares its class through topic_qos; the class reaches the verdict
    // via reliability_hint_of.
    const plexus::topic_qos reliable_qos{.reliability = pio::reliability::reliable};
    const plexus::topic_qos best_effort_qos{.reliability = pio::reliability::best_effort};

    const pio::endpoint udp_ep{"udp", "127.0.0.1:5000"};   // best_effort path
    const pio::endpoint tcp_ep{"tcp", "127.0.0.1:5000"};   // reliable stream
    const pio::endpoint udpr_ep{"udpr", "127.0.0.1:5000"}; // reliable-datagram opt-in

    for(int iter = 0; iter < k_reps; ++iter)
    {
        const auto reliable_hint    = sel.reliability_hint_of(reliable_qos.reliability);
        const auto best_effort_hint = sel.reliability_hint_of(best_effort_qos.reliability);

        // The declared reliable class steers the verdict: refused on the best_effort
        // path, admissible on the reliable ones (udpr never downgraded to bare udp).
        REQUIRE(sel.reliability_class(udp_ep, reliable_hint) == verdict::downgrade_refused);
        REQUIRE(sel.reliability_class(tcp_ep, reliable_hint) == verdict::admissible);
        REQUIRE(sel.reliability_class(udpr_ep, reliable_hint) == verdict::admissible);

        // A best_effort topic asks for no guarantee — admissible on any path.
        REQUIRE(sel.reliability_class(udp_ep, best_effort_hint) == verdict::admissible);
    }
}

TEST_CASE("transport steering: the selector verdict mirrors the engine reliability gate for every "
          "scheme",
          "[steering][reliability][mirror]")
{
    pio::transport_selector sel;
    using verdict = pio::reliability_admissibility;

    // The two enforcement points (publisher-side selector, subscriber-side engine gate)
    // are lock-step for EVERY scheme — the unknown "ws" row pins the fail-closed case
    // (downgrade_refused AND scheme_is_reliable false: they AGREE).
    for(int iter = 0; iter < k_reps; ++iter)
        for(std::string_view scheme : {"udp", "dtls", "tcp", "tls", "udpr", "unix", "inproc", "ws"})
        {
            const pio::endpoint ep{std::string{scheme}, "addr"};
            const auto          hint       = sel.reliability_hint_of(pio::reliability::reliable);
            const bool          admissible = sel.reliability_class(ep, hint) == verdict::admissible;
            REQUIRE(admissible == pio::scheme_is_reliable(ep.scheme));
        }
}

TEST_CASE("transport steering: a declared dispatch hint observably steers the class verdict",
          "[steering][dispatch]")
{
    pio::transport_selector sel;
    using pio::shm::dispatch_hint;

    const plexus::topic_qos hinted_qos{.dispatch = dispatch_hint::frequent};
    const plexus::topic_qos hintless_qos{.dispatch = dispatch_hint::none};

    const pio::endpoint unix_ep{"unix", "/tmp/s"};
    const pio::endpoint tcp_ep{"tcp", "127.0.0.1:5000"};

    for(int iter = 0; iter < k_reps; ++iter)
    {
        // The declared dispatch field observably changes the local fast-path verdict.
        REQUIRE(sel.dispatch_class(unix_ep, hinted_qos.dispatch) !=
                sel.dispatch_class(unix_ep, hintless_qos.dispatch));

        // For a remote peer the verdict records the documented advisory preference.
        REQUIRE(sel.dispatch_class(tcp_ep, hinted_qos.dispatch));
        REQUIRE_FALSE(sel.dispatch_class(tcp_ep, hintless_qos.dispatch));

        // shm_eligible_for is preserved beside the generalized verdict.
        REQUIRE(sel.shm_eligible_for(unix_ep, hinted_qos.dispatch));
        REQUIRE_FALSE(sel.shm_eligible_for(tcp_ep, hinted_qos.dispatch));
    }
}
