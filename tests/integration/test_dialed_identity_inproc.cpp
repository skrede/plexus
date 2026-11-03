#include "test_dialed_identity_common.h"

#include "plexus/io/endpoint_id.h"
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

TEST_CASE("dialed identity: a rejected slot releases its endpoint to the identity now answering there", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    t.net.a.note_peer(t.net.id_b, t.net.ep_b);
    t.net.a.reach(t.net.id_b);
    t.net.drive();
    const auto &c = t.rec.for_peer(t.net.id_b);
    CHECK(c.connected == 1);
    CHECK(c.rejected == 0);
    CHECK(t.net.a.is_connected(t.net.id_b));
    CHECK(t.net.a.registry().driver_for(t.advertised).redial_denied());
}

TEST_CASE("dialed identity: a fresh card for the rejected identity dials its own slot, not the one now connected", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    t.net.a.note_peer(t.net.id_b, t.net.ep_b);
    t.net.a.reach(t.net.id_b);
    t.net.drive();
    REQUIRE(t.net.a.is_connected(t.net.id_b));
    t.net.a.note_peer(t.advertised, t.net.ep_b);
    t.net.a.reach(t.advertised);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 2);
    const auto &c = t.rec.for_peer(t.net.id_b);
    CHECK(c.connected == 1);
    CHECK(c.disconnected == 0);
    CHECK(c.reconnected == 0);
    CHECK(t.net.a.is_connected(t.net.id_b));
}

TEST_CASE("dialed identity: a dial that fails at a released endpoint is reported to the slot that dialed it", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.a.add_observer(rec);
    const auto stale = make_id(0x10);
    net.a.note_peer(stale, net.ep_b);
    net.a.reach(stale);
    net.drive();
    REQUIRE(rec.for_peer(stale).rejected == 1);
    net.transport_b.close();
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.drive();
    net.transport_b.listen(net.ep_b);
    net.advance(k_ceiling);
    CHECK(rec.for_peer(net.id_b).connected == 1);
    CHECK(net.a.is_connected(net.id_b));
}

TEST_CASE("dialed identity: an endpoint dialed after a rejection hangs up the slot that dialed it", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.a.add_observer(rec);
    const auto stale = make_id(0x01);
    const auto slot  = plexus::io::endpoint_id(net.ep_b);
    REQUIRE(stale < slot);
    net.a.note_peer(stale, net.ep_b);
    net.a.reach(stale);
    net.drive();
    REQUIRE(rec.for_peer(stale).rejected == 1);
    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(slot));
    net.a.hang_up(net.ep_b);
    net.drive();
    CHECK(!net.a.is_connected(slot));
}

TEST_CASE("dialed identity: a released slot that completes again takes its endpoint back", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    t.net.transport_b.close();
    transport_t transport_c{t.net.ex, t.net.bus};
    engine c(transport_c, t.net.ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, t.net.sink, false);
    c.listen(t.net.ep_b);
    t.net.a.note_peer(t.advertised, t.net.ep_b);
    t.net.a.reach(t.advertised);
    t.net.drive();
    REQUIRE(t.net.a.is_connected(t.advertised));
    const auto other = make_id(0xD4);
    t.net.a.note_peer(other, t.net.ep_b);
    t.net.a.reach(other);
    t.net.drive();
    CHECK(t.rec.for_peer(other).rejected == 0);
    CHECK(t.net.a.is_connected(t.advertised));
}
