// Constrained-target seam proof: a host translation unit whose sole job is to fire
// the freertos_policy compile-time gate and prove the generic core binds against it
// UNCHANGED. Including freertos_policy.h fires static_assert(Policy<freertos_policy>)
// at compile time — the load-bearing proof that a fully non-asio substrate (a
// cooperative super-loop executor, a tick-backed timer, a no-op transport-free
// channel, a fixed in-place byte_owner) satisfies the single seam the engine is
// written against. The redundant local assert is an explicit witness at the test
// site; the runtime TEST_CASEs prove the generic core instantiates against the
// substrate with no I/O and no background thread.

#include "plexus/freertos/freertos_policy.h"

#include "plexus/io/peer_context.h"
#include "plexus/wire_bytes.h"
#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

// The explicit witness at the test site (mirrors the gate in the header).
static_assert(plexus::Policy<plexus::freertos::freertos_policy>, "freertos_policy must satisfy Policy at the seam-test site");

TEST_CASE("the generic core binds against the constrained-target Policy unchanged", "[seam]")
{
    // peer_context is the lightest generic-core type templated on Policy: a pure
    // value bundle guarded by `requires plexus::Policy<Policy>`, so instantiating it
    // re-proves the seam THROUGH the generic core — not just at the policy header.
    // It is default-constructible and needs no live channel (it holds a null
    // unique_ptr<channel_type>), so binding it proves the core compiles against the
    // substrate with no transport and no background thread.
    plexus::io::peer_context<plexus::freertos::freertos_policy> ctx;

    REQUIRE(ctx.channel == nullptr);       // no live connection — transport-free
    REQUIRE_FALSE(ctx.has_ever_connected); // fresh record default
    REQUIRE_FALSE(ctx.same_host);          // fail-closed default

    // The channel_type alias the core derived from the Policy is the stub channel.
    static_assert(std::is_same_v<plexus::io::peer_context<plexus::freertos::freertos_policy>::channel_type, plexus::freertos::freertos_stub_channel>,
                  "the generic core derives the channel type straight from the Policy");
}

TEST_CASE("wire_bytes instantiates against the fixed in-place byte_owner", "[seam]")
{
    // The mcu_byte_owner is an empty tag at this stage (the substrate moves no real
    // bytes), so there is no liveness contract to assert — only that the receive
    // seam's carrier instantiates against it: a default-constructed empty view whose
    // owner type is the Policy's byte_owner.
    plexus::wire_bytes<plexus::freertos::freertos_policy::byte_owner> wb;

    REQUIRE(wb.empty());
    REQUIRE(wb.size() == 0);

    static_assert(std::is_same_v<decltype(wb)::owner_type, plexus::freertos::mcu_byte_owner>, "the carrier binds the Policy-selected in-place owner");
}
