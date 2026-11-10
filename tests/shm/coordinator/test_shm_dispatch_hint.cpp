#include "plexus/io/dispatch_hint.h"
#include "plexus/io/transport_selector.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

// The dispatch_hint FLAGS type + the selector's locality x dispatch
// SHM-eligibility decision. dispatch_hint is a bitmask modeled on
// locality.h: none = 0 is the absence; any_set(h) is true iff any bit is set;
// the operators compose. topic_qos carries it (and max_message_bytes), both defaulting to
// "unset". The selector gates SHM on same-host AND a qualifying hint.

using plexus::io::any_set;
using plexus::io::dispatch_hint;

TEST_CASE("dispatch_hint: none is the absence; any bit is local_fast_eligible", "[shm][dispatch_hint]")
{
    REQUIRE_FALSE(any_set(dispatch_hint::none));
    REQUIRE(any_set(dispatch_hint::frequent));
    REQUIRE(any_set(dispatch_hint::large));
    REQUIRE(any_set(dispatch_hint::priority));
}

TEST_CASE("dispatch_hint: the operators compose like locality's bitflags", "[shm][dispatch_hint]")
{
    constexpr dispatch_hint both = dispatch_hint::frequent | dispatch_hint::large;

    // The union sets both bits and clears the third.
    REQUIRE(static_cast<std::uint8_t>(both) == (1u | 2u));
    REQUIRE(any_set(both));

    // operator& isolates a single bit: both shares frequent, not priority.
    REQUIRE((both & dispatch_hint::frequent) == dispatch_hint::frequent);
    REQUIRE((both & dispatch_hint::large) == dispatch_hint::large);
    REQUIRE((both & dispatch_hint::priority) == dispatch_hint::none);

    // A unioned mask remains eligible even after masking out one bit, as long as a
    // bit survives.
    REQUIRE(any_set(both & dispatch_hint::frequent));
}

TEST_CASE("topic_qos default-constructs with dispatch == none and max_message_bytes == 0", "[shm][dispatch_hint]")
{
    plexus::topic_qos qos{};

    REQUIRE(qos.dispatch == dispatch_hint::none);
    REQUIRE(qos.max_message_bytes == 0u);
    REQUIRE_FALSE(any_set(qos.dispatch));
}

TEST_CASE("selector: same-host x qualifying hint is SHM-eligible", "[shm][dispatch_hint]")
{
    plexus::io::transport_selector selector;

    const plexus::io::endpoint same_host{"unix", "/tmp/sock"};
    const plexus::io::endpoint inproc{"inproc", "node-a"};
    const plexus::io::endpoint remote{"tcp", "10.0.0.1:7000"};

    // Same-host (unix/inproc) AND a hint -> eligible.
    REQUIRE(selector.local_fast_eligible_for(same_host, dispatch_hint::frequent));
    REQUIRE(selector.local_fast_eligible_for(inproc, dispatch_hint::large | dispatch_hint::priority));

    // Same-host but no hint -> NOT eligible (stays on the local stream).
    REQUIRE_FALSE(selector.local_fast_eligible_for(same_host, dispatch_hint::none));

    // A hint over a remote peer -> NEVER eligible (a hint grants no cross-host reach).
    REQUIRE_FALSE(selector.local_fast_eligible_for(remote, dispatch_hint::frequent));
    REQUIRE_FALSE(selector.local_fast_eligible_for(remote, dispatch_hint::none));
}

TEST_CASE("selector: a topic_qos.dispatch drives the eligibility decision", "[shm][dispatch_hint]")
{
    plexus::io::transport_selector selector;
    const plexus::io::endpoint same_host{"unix", "/tmp/sock"};

    const plexus::topic_qos hinted{.dispatch = dispatch_hint::frequent};
    const plexus::topic_qos plain{};

    REQUIRE(selector.local_fast_eligible_for(same_host, hinted.dispatch));
    REQUIRE_FALSE(selector.local_fast_eligible_for(same_host, plain.dispatch));
}
