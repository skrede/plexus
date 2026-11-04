#include "test_dialed_identity_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/handshake_protocol.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <cstdint>
#include <string_view>

using namespace dialed_identity_fixture;
using plexus::io::handshake_outcome;

namespace {

constexpr std::string_view k_topic = "/dialed/topic";

}

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

TEST_CASE("dialed identity redial: a demand after a rejection does not dial until a fresh card", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    t.net.a.subscribe(t.advertised, k_topic);
    CHECK(t.net.a.registry().slot_count() == 0);
    t.net.drive();
    t.net.a.reach(t.advertised);
    t.net.drive();
    t.net.a.call(t.advertised, "/dialed/rpc", std::span<const std::byte>{}, [](auto, auto) {});
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 1);
    t.net.a.note_peer(t.advertised, t.net.ep_b);
    t.net.a.subscribe(t.advertised, k_topic);
    CHECK(t.net.a.registry().slot_count() == 1);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 2);
    CHECK(t.net.a.registry().slot_count() == 0);
}

TEST_CASE("dialed identity redial: under eager dialing each card dials once and a demand between cards dials nothing", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net(true);
    recording_observer rec;
    net.a.add_observer(rec);
    const auto stale = make_id(0xC3);
    net.a.note_peer(stale, net.ep_b);
    net.drive();
    REQUIRE(rec.for_peer(stale).rejected == 1);
    net.a.subscribe(stale, k_topic);
    net.drive();
    CHECK(rec.for_peer(stale).rejected == 1);
    for(std::int32_t cards = 2; cards <= 4; ++cards)
    {
        net.a.note_peer(stale, net.ep_b);
        net.drive();
        CHECK(rec.for_peer(stale).rejected == cards);
        net.a.subscribe(stale, k_topic);
        net.drive();
        CHECK(rec.for_peer(stale).rejected == cards);
    }
}

TEST_CASE("dialed identity redial: a card with a new endpoint authorizes one dial and no demand after it dials", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    const plexus::io::endpoint nowhere{"inproc", "node-nowhere"};
    t.net.a.note_peer(t.advertised, nowhere);
    t.net.a.subscribe(t.advertised, k_topic);
    t.net.drive();
    const auto after_card = t.rec.for_peer(t.advertised).rejected;
    t.net.a.subscribe(t.advertised, k_topic);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == after_card);
    CHECK(t.net.a.known().lookup(t.advertised) == nowhere);
}
