#include "test_dialed_identity_common.h"

#include "plexus/io/handshake_protocol.h"

#include <catch2/catch_test_macros.hpp>

using namespace dialed_identity_fixture;
using plexus::io::handshake_outcome;

TEST_CASE("dialed identity redial: a fresh card for the endpoint dials it again after a rejection", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    t.net.a.note_peer(t.advertised, t.net.ep_b);
    t.net.a.reach(t.advertised);
    t.net.drive();
    const auto &c = t.rec.for_peer(t.advertised);
    CHECK(c.rejected == 2);
    CHECK(c.last_reason == handshake_outcome::reject_identity);
    CHECK(!t.net.a.is_connected(t.advertised));
}
