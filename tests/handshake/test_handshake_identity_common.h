#ifndef HPP_GUARD_TESTS_HANDSHAKE_HANDSHAKE_IDENTITY_COMMON_H
#define HPP_GUARD_TESTS_HANDSHAKE_HANDSHAKE_IDENTITY_COMMON_H

#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

namespace handshake_identity_fixture {

constexpr plexus::node_id id_with_tail(std::uint8_t tail) noexcept
{
    plexus::node_id id{};
    id[15] = std::byte{tail};
    return id;
}

inline constexpr plexus::node_id k_self  = id_with_tail(0x10);
inline constexpr plexus::node_id k_peer  = id_with_tail(0x20);
inline constexpr plexus::node_id k_other = id_with_tail(0x30);

inline plexus::io::handshake_fsm_config config_for(plexus::node_id self) noexcept
{
    return plexus::io::handshake_fsm_config{.self_id = self, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::io::handshake_fsm_config config_expecting(plexus::node_id expected) noexcept
{
    auto cfg             = config_for(k_self);
    cfg.expected_peer_id = expected;
    return cfg;
}

inline plexus::wire::handshake_request request_from(plexus::node_id peer) noexcept
{
    return plexus::wire::handshake_request{.id                       = peer,
                                           .version_major            = 1,
                                           .version_minor            = 0,
                                           .compatible_version_major = 1,
                                           .compatible_version_minor = 0,
                                           .protocol_version         = plexus::wire::k_protocol_version,
                                           .fingerprint              = 0,
                                           .key_id                   = {},
                                           .own_nonce                = {},
                                           .cipher_offer             = 0,
                                           .chosen_cipher            = 0,
                                           .proof                    = {}};
}

inline plexus::wire::handshake_response response_from(plexus::node_id peer) noexcept
{
    const auto r = request_from(peer);
    return plexus::wire::handshake_response{.id                       = r.id,
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
                                            .status                   = plexus::wire::handshake_status::accepted};
}

inline void complete_inbound(plexus::io::handshake_fsm &fsm)
{
    REQUIRE(fsm.on_request(request_from(k_peer), true).action == plexus::io::fsm_action::complete);
}

inline void complete_outbound(plexus::io::handshake_fsm &fsm)
{
    fsm.on_dial_started();
    fsm.on_outbound_connected();
    REQUIRE(fsm.on_response(response_from(k_peer)).action == plexus::io::fsm_action::complete);
}

}

#endif
