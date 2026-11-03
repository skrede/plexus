#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::io::fsm_action;
using plexus::io::handshake_fsm;
using plexus::io::handshake_fsm_config;
using plexus::io::handshake_outcome;
using plexus::wire::handshake_request;
using plexus::wire::handshake_response;
using plexus::wire::handshake_status;
using plexus::wire::k_protocol_version;

namespace {

constexpr node_id id_with_tail(std::uint8_t tail) noexcept
{
    node_id id{};
    id[15] = std::byte{tail};
    return id;
}

constexpr node_id k_self  = id_with_tail(0x10);
constexpr node_id k_peer  = id_with_tail(0x20);
constexpr node_id k_other = id_with_tail(0x30);

handshake_fsm_config config_for(node_id self) noexcept
{
    return handshake_fsm_config{.self_id = self, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

handshake_fsm_config config_expecting(node_id expected) noexcept
{
    auto cfg             = config_for(k_self);
    cfg.expected_peer_id = expected;
    return cfg;
}

handshake_request request_from(node_id peer) noexcept
{
    return handshake_request{.id                       = peer,
                             .version_major            = 1,
                             .version_minor            = 0,
                             .compatible_version_major = 1,
                             .compatible_version_minor = 0,
                             .protocol_version         = k_protocol_version,
                             .fingerprint              = 0,
                             .key_id                   = {},
                             .own_nonce                = {},
                             .cipher_offer             = 0,
                             .chosen_cipher            = 0,
                             .proof                    = {}};
}

handshake_response response_from(node_id peer) noexcept
{
    const auto r = request_from(peer);
    return handshake_response{.id                       = r.id,
                              .version_major            = r.version_major,
                              .version_minor            = r.version_minor,
                              .compatible_version_major = r.compatible_version_major,
                              .compatible_version_minor = r.compatible_version_minor,
                              .protocol_version         = r.protocol_version,
                              .fingerprint              = r.fingerprint,
                              .key_id                   = r.key_id,
                              .own_nonce                = r.own_nonce,
                              .cipher_offer             = r.cipher_offer,
                              .chosen_cipher            = r.chosen_cipher,
                              .proof                    = r.proof,
                              .status                   = handshake_status::accepted};
}

void complete_inbound(handshake_fsm &fsm)
{
    REQUIRE(fsm.on_request(request_from(k_peer), true).action == fsm_action::complete);
}

void complete_outbound(handshake_fsm &fsm)
{
    fsm.on_dial_started();
    fsm.on_outbound_connected();
    REQUIRE(fsm.on_response(response_from(k_peer)).action == fsm_action::complete);
}

}

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
