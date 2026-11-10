#include "plexus/io/host_fingerprint.h"
#include "plexus/shm/region_naming.h"

#include "plexus/discovery/universe.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

// The same-host identity layer: the null-guarded fingerprint compare
// and the deterministic, direction-discriminated region naming. Both ends compute
// the region name independently with no exchange, so determinism + direction
// distinctness are the load-bearing properties.

using namespace plexus::shm;
using plexus::io::host_fingerprint;
using plexus::io::is_same_host;

namespace {

std::string name_of(std::string_view fqn, std::string_view ns = {}, ring_direction direction = ring_direction::request)
{
    return region_name_for(fqn, plexus::node_id{}, plexus::discovery::k_default_universe, direction, ns);
}

}

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

    const std::string req1 = name_of(fqn);
    const std::string req2 = name_of(fqn);
    const std::string resp = name_of(fqn, {}, ring_direction::response);

    // Deterministic: same input -> same name across calls.
    REQUIRE(req1 == req2);
    REQUIRE(name_of(fqn, {}, ring_direction::response) == name_of(fqn, {}, ring_direction::response));

    // Direction-discriminated: request and response name distinct rings.
    REQUIRE(req1 != resp);

    // A different fqn names a different region.
    REQUIRE(name_of("robot/telemetry/gps") != req1);
}

TEST_CASE("same_host: the region namespace isolates names", "[shm][same_host][namespace]")
{
    const std::string fqn = "robot/telemetry/imu";

    // CONVERGENCE: the SAME namespace + SAME topic -> the SAME name (both peers meet it).
    REQUIRE(name_of(fqn, "app-a") == name_of(fqn, "app-a"));

    // ISOLATION: DIFFERENT namespaces, SAME topic -> DISTINCT names (no collision), and a
    // non-empty namespace differs from the empty one.
    REQUIRE(name_of(fqn, "app-a") != name_of(fqn, "app-b"));
    REQUIRE(name_of(fqn, "app-a") != name_of(fqn));

    // The namespace stays direction-discriminated within one namespace.
    REQUIRE(name_of(fqn, "app-a") != name_of(fqn, "app-a", ring_direction::response));
}

TEST_CASE("same_host: a region name is bare lowercase hex", "[shm][same_host]")
{
    const std::string name = name_of("any/topic");

    REQUIRE(name.size() == 16);
    for(char c : name)
        REQUIRE(((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')));
}
