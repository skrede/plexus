// Mixed-version interop oracle for the session-handshake version gate. A peer whose
// protocol_version differs from this build's k_protocol_version must be rejected
// outright: the FSM returns fsm_action::abort with the reject_version outcome, which
// the bridge maps to the version_incompatible status byte. This is the existing
// exact-match gate (handshake_fsm::validate) — the test pins it against a DIFFERENT
// version, NOT the literal current value, so it survives a later protocol bump
// unchanged (a future bump shifts k_protocol_version but k_protocol_version - 1 is
// still a mismatch).

#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::io::fsm_action;
using plexus::io::handshake_fsm;
using plexus::io::handshake_fsm_config;
using plexus::io::handshake_outcome;
using plexus::wire::handshake_request;
using plexus::wire::k_protocol_version;

namespace {

constexpr node_id id_with_tail(std::uint8_t tail) noexcept
{
    node_id id{};
    id[15] = std::byte{tail};
    return id;
}

handshake_fsm_config config_for(node_id self) noexcept
{
    return handshake_fsm_config{.self_id = self, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A request that passes every gate EXCEPT for the protocol_version the caller sets.
handshake_request request_with_protocol(node_id peer, std::uint8_t protocol) noexcept
{
    return handshake_request{.id                       = peer,
                             .version_major            = 1,
                             .version_minor            = 0,
                             .compatible_version_major = 1,
                             .compatible_version_minor = 0,
                             .protocol_version         = protocol,
                             .fingerprint              = 0,
                             .key_id                   = {},
                             .own_nonce                = {},
                             .cipher_offer             = 0,
                             .chosen_cipher            = 0,
                             .proof                    = {}};
}

}

TEST_CASE("interop: a mismatched-protocol peer is aborted with reject_version", "[handshake][interop]")
{
    handshake_fsm fsm(config_for(id_with_tail(0xAA)));

    // Drive a peer one protocol version below this build's. The exact-match gate
    // rejects it regardless of the absolute value of k_protocol_version, so the
    // pin survives a future bump.
    const auto skewed = static_cast<std::uint8_t>(k_protocol_version - 1);
    const auto req    = request_with_protocol(id_with_tail(0xBB), skewed);

    const auto result = fsm.on_request(req, /*inbound_is_bootstrap=*/true);

    CHECK(result.action == fsm_action::abort);
    CHECK(result.outcome == handshake_outcome::reject_version);
}

TEST_CASE("interop: a matching-protocol peer clears the version gate", "[handshake][interop]")
{
    handshake_fsm fsm(config_for(id_with_tail(0xAA)));
    const auto req = request_with_protocol(id_with_tail(0xBB), k_protocol_version);

    const auto result = fsm.on_request(req, /*inbound_is_bootstrap=*/true);

    // A matching protocol does NOT abort on the version gate (it proceeds to the
    // inbound resolution path).
    CHECK(result.action != fsm_action::abort);
}
