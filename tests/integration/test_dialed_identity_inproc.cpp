#include "test_dialed_identity_common.h"

#include "plexus/io/handshake_protocol.h"

#include <catch2/catch_test_macros.hpp>

using namespace dialed_identity_fixture;
using plexus::io::handshake_outcome;

TEST_CASE("dialed identity: a dialed peer that proves another identity is rejected and never completes", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    const auto &c = t.rec.for_peer(t.advertised);
    CHECK(c.rejected == 1);
    CHECK(c.last_reason == handshake_outcome::reject_identity);
    CHECK(c.connected == 0);
    CHECK(!t.net.a.is_connected(t.advertised));
    CHECK(!t.net.a.is_connected(t.net.id_b));
}

TEST_CASE("dialed identity: a rejected dialed slot is not redialed by its backoff", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    auto &driver      = t.net.a.registry().driver_for(t.advertised);
    const auto before = t.net.a.attempt_count(t.advertised);
    CHECK(driver.redial_denied());
    driver.on_channel_dropped();
    t.net.advance(k_ceiling);
    t.net.advance(k_ceiling);
    CHECK(t.net.a.attempt_count(t.advertised) == before);
    CHECK(t.rec.for_peer(t.advertised).rejected == 1);
}

TEST_CASE("dialed identity: a dialed peer that proves the advertised identity completes", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.a.add_observer(rec);
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.drive();
    const auto &c = rec.for_peer(net.id_b);
    CHECK(c.connected == 1);
    CHECK(c.rejected == 0);
    CHECK(net.a.is_connected(net.id_b));
}
