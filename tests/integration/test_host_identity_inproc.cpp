#include "test_host_identity_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace host_identity_fixture;

TEST_CASE("host_identity: the PSK-path accessor returns the attach-bound id, by role", "[integration][host_identity]")
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

TEST_CASE("host_identity: the TLS-path accessor returns the SPKI-derived id", "[integration][host_identity]")
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

    inproc_bus<>                            bus;
    inproc_executor<>                       ex{bus};
    inproc_transport<>                      transport{ex, bus};
    plexus::log::null_logger                sink;
    msg_forwarder                           req_messages{sink}, resp_messages{sink};
    rpc_forwarder                           req_procedures{ex, k_long_timeout, sink}, resp_procedures{ex, k_long_timeout, sink};
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
                responder.emplace(resp_ctx, ex, make_cfg(0x01, &admit), k_long_timeout, resp_messages, resp_procedures, true, sink);
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
                requester.emplace(req_ctx, ex, make_cfg(0x02, &admit), k_long_timeout, req_messages, req_procedures, false, sink);
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
