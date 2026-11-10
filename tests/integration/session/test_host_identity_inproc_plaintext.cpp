#include "test_host_identity_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace host_identity_fixture;

TEST_CASE("host_identity: a plaintext (no security posture) attach has no authenticated identity", "[integration][host_identity]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};
    plexus::log::null_logger sink;
    msg_forwarder req_messages{sink}, resp_messages{sink};
    rpc_forwarder req_procedures{ex, k_long_timeout, sink}, resp_procedures{ex, k_long_timeout, sink};
    plexus::io::peer_context<inproc_policy> req_ctx, resp_ctx;
    std::optional<session> requester, responder;

    transport.on_accepted(
            [&](std::unique_ptr<inproc_channel<>> ch)
            {
                resp_ctx.channel   = std::move(ch);
                resp_ctx.node_name = "requester-node";
                resp_ctx.peer_id   = make_id(0x02);
                responder.emplace(resp_ctx, ex, make_cfg(0x01, nullptr), k_long_timeout, resp_messages, resp_procedures, true, sink);
                responder->start();
            });
    transport.on_dialed(
            [&](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
            {
                req_ctx.channel   = std::move(ch);
                req_ctx.node_name = "responder-node";
                req_ctx.peer_id   = make_id(0x01);
                requester.emplace(req_ctx, ex, make_cfg(0x02, nullptr), k_long_timeout, req_messages, req_procedures, false, sink);
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
