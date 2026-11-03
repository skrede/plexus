#include "test_handshake_identity_common.h"

#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using plexus::node_id;
using plexus::io::fsm_action;
using plexus::io::handshake_fsm;
using plexus::io::handshake_outcome;
using plexus::wire::k_protocol_version;

using namespace handshake_identity_fixture;

TEST_CASE("identity: a request naming another peer after completion aborts and keeps the proven id", "[handshake][identity]")
{
    handshake_fsm fsm(config_for(k_self));
    complete_inbound(fsm);
    const auto r = fsm.on_request(request_from(k_other), true);
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_identity);
    CHECK(fsm.last_seen_peer_id() == k_peer);
}

TEST_CASE("identity: a request from the proven peer after completion is answered again", "[handshake][identity]")
{
    handshake_fsm fsm(config_for(k_self));
    complete_inbound(fsm);
    const auto r = fsm.on_request(request_from(k_peer), true);
    CHECK(r.action == fsm_action::send_response);
    CHECK(r.outcome == handshake_outcome::accept_inbound);
    CHECK(fsm.last_seen_peer_id() == k_peer);
}

TEST_CASE("identity: a response naming another peer after completion aborts and keeps the proven id", "[handshake][identity]")
{
    handshake_fsm fsm(config_for(k_self));
    complete_outbound(fsm);
    const auto r = fsm.on_response(response_from(k_other));
    CHECK(r.action == fsm_action::abort);
    CHECK(fsm.last_seen_peer_id() == k_peer);
}

TEST_CASE("identity: a response from the proven peer after completion takes no action", "[handshake][identity]")
{
    handshake_fsm fsm(config_for(k_self));
    complete_outbound(fsm);
    CHECK(fsm.on_response(response_from(k_peer)).action == fsm_action::none);
    CHECK(fsm.last_seen_peer_id() == k_peer);
}

TEST_CASE("identity: a request refused for its protocol version writes no identity", "[handshake][identity]")
{
    handshake_fsm fsm(config_for(k_self));
    auto req             = request_from(k_peer);
    req.protocol_version = static_cast<std::uint8_t>(k_protocol_version + 1);
    const auto r         = fsm.on_request(req, true);
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_version);
    CHECK(fsm.last_seen_peer_id() == node_id{});
}

TEST_CASE("identity: a response from a peer other than the expected one aborts with reject_identity", "[handshake][identity]")
{
    handshake_fsm fsm(config_expecting(k_peer));
    fsm.on_dial_started();
    fsm.on_outbound_connected();
    const auto r = fsm.on_response(response_from(k_other));
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_identity);
    CHECK(fsm.last_seen_peer_id() == node_id{});
}

TEST_CASE("identity: a request on a slot expecting another peer aborts with reject_identity", "[handshake][identity]")
{
    handshake_fsm fsm(config_expecting(k_peer));
    const auto r = fsm.on_request(request_from(k_other), true);
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_identity);
    CHECK(fsm.last_seen_peer_id() == node_id{});
}

TEST_CASE("identity: a config with no expected peer accepts any peer", "[handshake][identity]")
{
    handshake_fsm inbound(config_for(k_self));
    CHECK(inbound.on_request(request_from(k_other), true).action == fsm_action::complete);
    handshake_fsm outbound(config_for(k_self));
    outbound.on_dial_started();
    outbound.on_outbound_connected();
    CHECK(outbound.on_response(response_from(k_other)).action == fsm_action::complete);
}
