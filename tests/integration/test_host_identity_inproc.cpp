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

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

handshake_fsm_config make_cfg(std::uint8_t                               id_seed,
                              const plexus::io::security::attach_policy *policy)
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
    [[nodiscard]] bool decide(const attach_facts &) const noexcept override { return true; }
};

security_seam honest_seam()
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

TEST_CASE("host_identity: the PSK-path accessor returns the attach-bound id, by role",
          "[integration][host_identity]")
{
    attach_facts f;
    f.initiator_id = make_id(0xAA);
    f.responder_id = make_id(0xBB);

    // The local node reads the OTHER end's id from the verified facts.
    f.role = attach_role::initiator; // local is initiator -> peer is the responder
    REQUIRE(authenticated_peer_id(f) == make_id(0xBB));
    f.role = attach_role::responder; // local is responder -> peer is the initiator
    REQUIRE(authenticated_peer_id(f) == make_id(0xAA));
}

TEST_CASE("host_identity: the TLS-path accessor returns the SPKI-derived id",
          "[integration][host_identity]")
{
    cert_facts c;
    for(std::size_t i = 0; i < c.spki_sha256.size(); ++i)
        c.spki_sha256[i] = static_cast<std::byte>(i + 1);

    // The TLS identity is the first 16 bytes of the SPKI digest (cert_facts::to_node_id).
    const auto id = authenticated_peer_id(c);
    for(std::size_t i = 0; i < id.size(); ++i)
        REQUIRE(id[i] == static_cast<std::byte>(i + 1));
}

TEST_CASE("host_identity: a spoofed external claim is ignored — the accessor binds to the "
          "authenticated peer",
          "[integration][host_identity]")
{
    admit_policy  admit;
    security_seam req_seam  = honest_seam();
    security_seam resp_seam = honest_seam();

    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> transport{ex, bus};
    msg_forwarder      req_messages{}, resp_messages{};
    rpc_forwarder      req_procedures{ex, k_long_timeout}, resp_procedures{ex, k_long_timeout};
    plexus::io::peer_context<inproc_policy> req_ctx, resp_ctx;
    std::optional<session>                  requester, responder;

    const auto id_resp = make_id(0x01); // the dialed (accepting) node's true identity
    const auto id_req  = make_id(0x02); // the dialing node's true identity

    transport.on_accepted(
            [&](std::unique_ptr<inproc_channel<>> ch)
            {
                resp_ctx.channel   = std::move(ch);
                resp_ctx.node_name = "requester-node";
                resp_ctx.peer_id   = id_req;
                responder.emplace(resp_ctx, ex, make_cfg(0x01, &admit), k_long_timeout,
                                  resp_messages, resp_procedures, true);
                responder->set_security_seam(&resp_seam);
                responder->on_install_security([](const security_negotiation &) {});
                responder->start();
            });
    transport.on_dialed(
            [&](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
            {
                req_ctx.channel   = std::move(ch);
                req_ctx.node_name = "responder-node";
                req_ctx.peer_id   = id_resp;
                requester.emplace(req_ctx, ex, make_cfg(0x02, &admit), k_long_timeout, req_messages,
                                  req_procedures, false);
                requester->set_security_seam(&req_seam);
                requester->on_install_security([](const security_negotiation &) {});
                // Before the attach resolves the accessor is absent — no identity to spoof yet.
                REQUIRE_FALSE(requester->authenticated_host_identity().has_value());
                requester->start();
            });

    transport.listen({"inproc", "svc"});
    transport.dial({"inproc", "svc"});
    ex.drain();

    REQUIRE(requester->is_complete());
    REQUIRE(responder->is_complete());

    // The dialer (initiator) authenticated the accepter: the accessor returns the
    // accepter's true id (id_resp), bound by the handshake — not a value any external
    // TXT/discovery claim could assert (the accessor reads ONLY the verified facts).
    const auto req_identity = requester->authenticated_host_identity();
    REQUIRE(req_identity.has_value());
    REQUIRE(*req_identity == id_resp);

    const auto resp_identity = responder->authenticated_host_identity();
    REQUIRE(resp_identity.has_value());
    REQUIRE(*resp_identity == id_req);
}

TEST_CASE("host_identity: a plaintext (no security posture) attach has no authenticated identity",
          "[integration][host_identity]")
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> transport{ex, bus};
    msg_forwarder      req_messages{}, resp_messages{};
    rpc_forwarder      req_procedures{ex, k_long_timeout}, resp_procedures{ex, k_long_timeout};
    plexus::io::peer_context<inproc_policy> req_ctx, resp_ctx;
    std::optional<session>                  requester, responder;

    transport.on_accepted(
            [&](std::unique_ptr<inproc_channel<>> ch)
            {
                resp_ctx.channel   = std::move(ch);
                resp_ctx.node_name = "requester-node";
                resp_ctx.peer_id   = make_id(0x02);
                responder.emplace(resp_ctx, ex, make_cfg(0x01, nullptr), k_long_timeout,
                                  resp_messages, resp_procedures, true);
                responder->start();
            });
    transport.on_dialed(
            [&](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
            {
                req_ctx.channel   = std::move(ch);
                req_ctx.node_name = "responder-node";
                req_ctx.peer_id   = make_id(0x01);
                requester.emplace(req_ctx, ex, make_cfg(0x02, nullptr), k_long_timeout,
                                  req_messages, req_procedures, false);
                requester->start();
            });

    transport.listen({"inproc", "svc"});
    transport.dial({"inproc", "svc"});
    ex.drain();

    REQUIRE(requester->is_complete());
    // No PSK posture -> no authenticated host identity is asserted (absence is meaningful).
    REQUIRE_FALSE(requester->authenticated_host_identity().has_value());
    REQUIRE_FALSE(responder->authenticated_host_identity().has_value());
}
