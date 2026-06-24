#include "plexus/native/machine_fingerprint.h"

#include "plexus/io/host_fingerprint.h"
#include "plexus/io/shm/ring_layout.h"

#include <catch2/catch_test_macros.hpp>

namespace pio = plexus::io::shm;

TEST_CASE("shm.same_host_roundtrip the machine fingerprint is deterministic and non-null",
          "[shm][same_host_roundtrip]")
{
    const plexus::io::host_fingerprint a = plexus::native::read_machine_fingerprint();
    const plexus::io::host_fingerprint b = plexus::native::read_machine_fingerprint();

    // Determinism within a host: two reads agree (the inputs are stable -- NOT a
    // cached static).
    REQUIRE(a == b);
    // Non-null on a real host (the fail-closed same-host guard treats null as never
    // same-host, so a real host MUST produce a non-null value).
    REQUIRE_FALSE(a.is_null());
    // It IS the local fingerprint: a peer carrying the same value reads same-host.
    REQUIRE(plexus::io::is_same_host(b, a));
    // A null (unidentified) peer is NEVER same-host (fail-closed).
    REQUIRE_FALSE(plexus::io::is_same_host(plexus::io::host_fingerprint{}, a));
}
