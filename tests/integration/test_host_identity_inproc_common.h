#ifndef HPP_GUARD_TESTS_INTEGRATION_HOST_IDENTITY_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_HOST_IDENTITY_INPROC_COMMON_H

// The host-identity accessor oracle: the authenticated peer's host identity is
// read from the security step that admitted it — the SPKI-derived node_id for a TLS
// peer, the attach-bound node_id for a PSK peer — NEVER a self-asserted TXT/wire claim.
// The pure free-function legs prove the derivation; the bridge leg proves the session
// accessor latches the AUTHENTICATED peer id (and is absent before the attach resolves /
// when no security posture is engaged), so a forged external identity claim cannot
// substitute for it.

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/host_identity.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/security/attach_policy.h"
#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/cert_facts.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::handshake_fsm_config;
using plexus::io::security_seam;
using plexus::io::security_negotiation;
using plexus::io::authenticated_peer_id;
using plexus::io::security::attach_facts;
using plexus::io::security::attach_role;
using plexus::io::security::cert_facts;
using session       = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace host_identity_fixture {

constexpr auto k_long_timeout = std::chrono::hours(1);

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed, const plexus::io::security::attach_policy *policy)
{
    return handshake_fsm_config{.self_id                  = make_id(id_seed),
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0,
                                .local_fingerprint        = {},
                                .attach_policy            = policy};
}

struct admit_policy final : public plexus::io::security::attach_policy
{
    [[nodiscard]] bool decide(const attach_facts &) const noexcept override
    {
        return true;
    }
};

inline security_seam honest_seam()
{
    security_seam s;
    s.transcript = [](std::span<const std::byte>, std::span<std::byte, 32> out)
    {
        for(auto &b : out)
            b = std::byte{0};
        return true;
    };
    return s;
}

}

#endif
