#include "test_handshake_identity_common.h"

#include "plexus/io/handshake_fsm.h"
#include "plexus/io/host_fingerprint.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using plexus::node_id;
using plexus::io::fsm_action;
using plexus::io::handshake_fsm;
using plexus::io::handshake_outcome;
using plexus::io::host_fingerprint;
using plexus::wire::handshake_status;

using namespace handshake_identity_fixture;

namespace {

constexpr std::uint64_t k_fingerprint = 0x1234;

handshake_fsm armed_outbound()
{
    handshake_fsm fsm(config_for(k_self));
    fsm.on_dial_started();
    fsm.on_outbound_connected();
    return fsm;
}

void require_no_identity(const handshake_fsm &fsm)
{
    CHECK(fsm.last_seen_peer_id() == node_id{});
    CHECK(fsm.last_seen_peer_fingerprint() == host_fingerprint{});
}

}

TEST_CASE("refusal: a request below the version window records no identity", "[handshake][refusal]")
{
    handshake_fsm fsm(config_for(k_self));
    auto req          = request_from(k_peer);
    req.fingerprint   = k_fingerprint;
    req.version_major = 0;
    const auto r      = fsm.on_request(req, true);
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_version);
    require_no_identity(fsm);
}

TEST_CASE("refusal: an accepted response below the version window records no identity", "[handshake][refusal]")
{
    auto fsm           = armed_outbound();
    auto resp          = response_from(k_peer);
    resp.fingerprint   = k_fingerprint;
    resp.version_major = 0;
    const auto r       = fsm.on_response(resp);
    CHECK(r.action == fsm_action::abort);
    CHECK(r.outcome == handshake_outcome::reject_version);
    require_no_identity(fsm);
}

TEST_CASE("refusal: a response with a non-accepted status records no identity", "[handshake][refusal]")
{
    for(const auto status : {handshake_status::version_incompatible, handshake_status::identity_conflict, handshake_status::rejected_unknown, handshake_status::unauthorized})
    {
        INFO("status " << static_cast<std::uint32_t>(status));
        auto fsm         = armed_outbound();
        auto resp        = response_from(k_peer);
        resp.fingerprint = k_fingerprint;
        resp.status      = status;
        const auto r     = fsm.on_response(resp);
        const auto want  = status == handshake_status::identity_conflict ? handshake_outcome::reject_identity : handshake_outcome::reject_version;
        CHECK(r.action == fsm_action::abort);
        CHECK(r.outcome == want);
        require_no_identity(fsm);
    }
}

TEST_CASE("refusal: an accepted request records the peer id and fingerprint", "[handshake][refusal]")
{
    handshake_fsm fsm(config_for(k_self));
    auto req        = request_from(k_peer);
    req.fingerprint = k_fingerprint;
    CHECK(fsm.on_request(req, true).action == fsm_action::complete);
    CHECK(fsm.last_seen_peer_id() == k_peer);
    CHECK(fsm.last_seen_peer_fingerprint() == host_fingerprint{k_fingerprint});
}

TEST_CASE("refusal: an accepted response records the peer id and fingerprint", "[handshake][refusal]")
{
    auto fsm         = armed_outbound();
    auto resp        = response_from(k_peer);
    resp.fingerprint = k_fingerprint;
    CHECK(fsm.on_response(resp).action == fsm_action::complete);
    CHECK(fsm.last_seen_peer_id() == k_peer);
    CHECK(fsm.last_seen_peer_fingerprint() == host_fingerprint{k_fingerprint});
}
