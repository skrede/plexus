#include "plexus/io/host_fingerprint.h"
#include "plexus/shm/region_naming.h"

#include <catch2/catch_test_macros.hpp>

#include <string>

// The same-host identity layer: the null-guarded fingerprint compare
// and the deterministic, direction-discriminated region naming. Both ends compute
// the region name independently with no exchange, so determinism + direction
// distinctness are the load-bearing properties.

using namespace plexus::shm;
using plexus::io::host_fingerprint;
using plexus::io::is_same_host;

TEST_CASE("same_host: a null peer fingerprint is never same-host", "[shm][same_host]")
{
    const host_fingerprint local{0x1234};
    const host_fingerprint null_peer{0};

    REQUIRE(null_peer.is_null());
    // The equality check ALONE would make two null fingerprints same-host; the
    // null-guard fails closed.
    REQUIRE_FALSE(is_same_host(null_peer, local));
    REQUIRE_FALSE(is_same_host(host_fingerprint{0}, host_fingerprint{0}));
}

TEST_CASE("same_host: equal non-null fingerprints are same-host; distinct are not", "[shm][same_host]")
{
    const host_fingerprint a{0xdeadbeef};
    const host_fingerprint same{0xdeadbeef};
    const host_fingerprint other{0xfeedface};

    REQUIRE(is_same_host(a, same));
    REQUIRE_FALSE(is_same_host(a, other));
}

TEST_CASE("same_host: region naming is deterministic and direction-discriminated", "[shm][same_host]")
{
    const std::string fqn = "robot/telemetry/imu";

    const std::string req1 = region_name_for(fqn, ring_direction::request);
    const std::string req2 = region_name_for(fqn, ring_direction::request);
    const std::string resp = region_name_for(fqn, ring_direction::response);

    // Deterministic: same input -> same name across calls.
    REQUIRE(req1 == req2);
    REQUIRE(region_name_for(fqn, ring_direction::response) == region_name_for(fqn, ring_direction::response));

    // Direction-discriminated: request and response name distinct rings.
    REQUIRE(req1 != resp);

    // A different fqn names a different region.
    REQUIRE(region_name_for("robot/telemetry/gps", ring_direction::request) != req1);
}

TEST_CASE("same_host: the region namespace isolates names and an empty namespace is back-compat", "[shm][same_host][namespace]")
{
    const std::string fqn = "robot/telemetry/imu";

    // BACK-COMPAT: an empty namespace yields byte-identical names to the namespace-less
    // overload, in BOTH directions — so every existing region name is unchanged.
    REQUIRE(region_name_for(fqn, ring_direction::request, "") == region_name_for(fqn, ring_direction::request));
    REQUIRE(region_name_for(fqn, ring_direction::response, "") == region_name_for(fqn, ring_direction::response));

    // CONVERGENCE: the SAME namespace + SAME topic -> the SAME name (both peers meet it).
    REQUIRE(region_name_for(fqn, ring_direction::request, "app-a") == region_name_for(fqn, ring_direction::request, "app-a"));

    // ISOLATION: DIFFERENT namespaces, SAME topic -> DISTINCT names (no collision), and a
    // non-empty namespace differs from the shared-by-topic default.
    REQUIRE(region_name_for(fqn, ring_direction::request, "app-a") != region_name_for(fqn, ring_direction::request, "app-b"));
    REQUIRE(region_name_for(fqn, ring_direction::request, "app-a") != region_name_for(fqn, ring_direction::request));

    // The namespace stays direction-discriminated within one namespace.
    REQUIRE(region_name_for(fqn, ring_direction::request, "app-a") != region_name_for(fqn, ring_direction::response, "app-a"));
}

TEST_CASE("same_host: a region name is bare lowercase hex", "[shm][same_host]")
{
    const std::string name = region_name_for("any/topic", ring_direction::request);

    REQUIRE(name.size() == 16);
    for(char c : name)
        REQUIRE(((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')));
}
