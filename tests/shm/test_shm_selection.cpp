#include "plexus/io/shm/shm_selection.h"

#include <catch2/catch_test_macros.hpp>

// The shared-memory selection decision: a same-host pair with a qualifying hint
// resolves to the shared-memory medium; a same-host pair with no hint falls back to
// the stream; a cross-host pair is never shared-memory regardless of hint.

using namespace plexus::io::shm;

namespace {
constexpr host_fingerprint k_host_a{0xa11};
constexpr host_fingerprint k_host_b{0xb22};
constexpr host_fingerprint k_null{0};
}

TEST_CASE("selection: a same-host pair with a hint is shared-memory", "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_a, k_host_a, dispatch_hint::frequent) ==
            same_host_medium::shm);
    REQUIRE(select_same_host_medium(k_host_a, k_host_a,
                                    dispatch_hint::large | dispatch_hint::priority) ==
            same_host_medium::shm);
}

TEST_CASE("selection: a same-host pair with no hint falls back to the stream", "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_a, k_host_a, dispatch_hint::none) ==
            same_host_medium::stream);
}

TEST_CASE("selection: a cross-host pair is never shared-memory regardless of hint",
          "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_b, k_host_a, dispatch_hint::frequent) ==
            same_host_medium::stream);
    REQUIRE(select_same_host_medium(k_host_b, k_host_a, dispatch_hint::none) ==
            same_host_medium::stream);

    // A null peer (advertised nothing / forged zero) is never shared-memory.
    REQUIRE(select_same_host_medium(k_null, k_host_a, dispatch_hint::frequent) ==
            same_host_medium::stream);
}
