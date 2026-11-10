// Relayed request/response transits end-to-end between two leaves with NO direct session: the caller
// route_selects the far leaf's via-relay candidate, wraps the request in a forwarded envelope onto the
// relay session, and the responder's reply — addressed by the requester origin, re-resolved through
// route_select at every hop — returns the responder's bytes. The relay carries both legs holding no
// per-correlation state.

#include "test_forward_relay_common.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstddef>
#include <optional>

using namespace forward_relay_fixture;
using plexus::wire::rpc_status;

TEST_CASE("forward_reqres: relayed req/res transits end-to-end through a stateless relay between two pathless leaves", "[integration][forward_reqres]")
{
    relay_line line;

    // The leaves are directly unreachable to each other — each crosses only by the via-relay candidate it
    // ingests from the relay's report.
    REQUIRE_FALSE(line.caller.is_connected(line.id_server));
    REQUIRE_FALSE(line.server.is_connected(line.id_caller));
    line.caller.ingest_peer_report(line.id_relay, reach_report(line.id_server));
    line.server.ingest_peer_report(line.id_relay, reach_report(line.id_caller));
    line.drive();

    // A responder that echoes its param back to the caller, proving the reply bytes transit the relay.
    std::string served_param;
    line.server.procedures().serve("demo.echo",
                                   [&](std::span<const std::byte> param, rpc_forwarder::reply_fn &reply) {
                                       served_param = to_string(param);
                                       reply(rpc_status::success, param);
                                   });

    std::optional<rpc_status> status;
    std::string response;
    line.caller.call(line.id_server, "demo.echo", as_bytes(std::string{"ping"}),
                     [&](rpc_status s, std::span<const std::byte> ret) {
                         status   = s;
                         response = to_string(ret);
                     });
    line.drive();

    REQUIRE(served_param == "ping");
    REQUIRE(status.has_value());
    REQUIRE(*status == rpc_status::success);
    REQUIRE(response == "ping");

    // Still no direct leaf-to-leaf session: the whole exchange rode the relay.
    REQUIRE_FALSE(line.caller.is_connected(line.id_server));
    REQUIRE_FALSE(line.server.is_connected(line.id_caller));
    // The relay never correlated the exchange — its procedure table stayed empty (stateless reply path).
    REQUIRE(line.relay.procedures().outstanding() == 0);
    REQUIRE(line.caller.forward_rpc_dropped_count() == 0);
    REQUIRE(line.server.forward_rpc_dropped_count() == 0);
}
