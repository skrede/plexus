#include "plexus/shm/machine_fingerprint.h"

#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/ring_layout.h"

#include <catch2/catch_test_macros.hpp>

namespace pio = plexus::io::shm;

TEST_CASE("shm.same_host_roundtrip the machine fingerprint is deterministic and non-null",
          "[shm][same_host_roundtrip]")
{
    const pio::host_fingerprint a = plexus::shm::read_machine_fingerprint();
    const pio::host_fingerprint b = plexus::shm::read_machine_fingerprint();

    // Determinism within a host: two reads agree (the inputs are stable -- NOT a
    // cached static).
    REQUIRE(a == b);
    // Non-null on a real host (the fail-closed same-host guard treats null as never
    // same-host, so a real host MUST produce a non-null value).
    REQUIRE_FALSE(a.is_null());
    // It IS the local fingerprint: a peer carrying the same value reads same-host.
    REQUIRE(pio::is_same_host(b, a));
    // A null (unidentified) peer is NEVER same-host (fail-closed).
    REQUIRE_FALSE(pio::is_same_host(pio::host_fingerprint{}, a));
}
