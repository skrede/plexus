// Reply routing is identity re-resolution, not path memory (D95.2). Two proofs:
//
//   (a) with an alternate relay present, retiring the relay the request rode still lets the reply
//       re-resolve through route_select at the responder and return via the alternate — the reply is
//       addressed by the requester origin, never pinned to the arrival session.
//   (b) a requester reachable by NO live route back correctly fails: route_select over empty candidates
//       drops the reply with a count, and the caller's deadline fires rpc_status::timeout. No route is
//       manufactured; the relay holds no per-correlation state either way.

#include "test_forward_relay_common.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

using namespace forward_relay_fixture;
using plexus::wire::rpc_status;

namespace {

// A four-node line: two independent relays each wired to both leaves, so the responder can reach the
// caller by either relay. The request rides relay-1's candidate; relay-2 is the standby the reply
// re-resolves onto once relay-1's return candidate is retired.
struct dual_relay_line
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t t_caller{ex, bus};
    transport_t t_r1{ex, bus};
    transport_t t_r2{ex, bus};
    transport_t t_server{ex, bus};
    plexus::log::null_logger sink;

    engine caller;
    engine relay1;
    engine relay2;
    engine server;

    node_id id_caller{make_id(0xC1)};
    node_id id_r1{make_id(0x21)};
    node_id id_r2{make_id(0x22)};
    node_id id_server{make_id(0x53)};
    endpoint ep_caller{"inproc", "rr-caller"};
    endpoint ep_r1{"inproc", "rr-relay1"};
    endpoint ep_r2{"inproc", "rr-relay2"};
    endpoint ep_server{"inproc", "rr-server"};

    dual_relay_line()
            : caller(t_caller, ex, make_cfg(0xC1), k_long_timeout, forever_cfg(), k_seed, sink)
            , relay1(t_r1, ex, make_cfg(0x21), k_long_timeout, forever_cfg(), k_seed, sink)
            , relay2(t_r2, ex, make_cfg(0x22), k_long_timeout, forever_cfg(), k_seed, sink)
            , server(t_server, ex, make_cfg(0x53), k_long_timeout, forever_cfg(), k_seed, sink)
    {
        caller.listen(ep_caller);
        relay1.listen(ep_r1);
        relay2.listen(ep_r2);
        server.listen(ep_server);
        for(engine *r : {&relay1, &relay2})
        {
            r->note_peer(id_caller, ep_caller);
            r->note_peer(id_server, ep_server);
            r->reach(id_caller);
            r->reach(id_server);
        }
        ex.drain();
    }

    void drive()
    {
        ex.drain();
    }
};

}

TEST_CASE("forward_reply_reresolve: a relayed reply re-resolves onto an alternate relay when the request's return candidate is retired", "[integration][forward_reply_reresolve]")
{
    dual_relay_line line;

    // The caller crosses only via relay-1; the responder can reach the caller via BOTH relays.
    line.caller.ingest_peer_report(line.id_r1, reach_report(line.id_server));
    line.server.ingest_peer_report(line.id_r1, reach_report(line.id_caller, /*seq=*/1));
    line.server.ingest_peer_report(line.id_r2, reach_report(line.id_caller, /*seq=*/1));
    line.drive();

    // A responder that defers its reply so the return route can change between request and reply.
    rpc_forwarder::reply_fn *deferred = nullptr;
    bool served                       = false;
    line.server.procedures().serve("demo.defer",
                                   [&](std::span<const std::byte>, rpc_forwarder::reply_fn &reply) {
                                       served   = true;
                                       deferred = &reply;
                                   });

    std::optional<rpc_status> status;
    std::string response;
    line.caller.call(line.id_server, "demo.defer", as_bytes(std::string{"q"}),
                     [&](rpc_status s, std::span<const std::byte> ret) {
                         status   = s;
                         response = to_string(ret);
                     });
    line.drive();

    REQUIRE(served);          // the request transited via relay-1 and reached the responder
    REQUIRE_FALSE(status);    // the reply is deferred, not yet emitted

    // Retire relay-1's route back to the caller (relay-1's withdrawal, or its death propagated as one):
    // the responder's only remaining return candidate is via relay-2.
    line.server.ingest_peer_report(line.id_r1, reach_report(line.id_caller, /*seq=*/2, wire::k_peer_report_withdrawal_flag));
    line.drive();

    // Emit the deferred reply: route_select at the responder now yields the relay-2 candidate, and the
    // reply returns end-to-end over the alternate path.
    const std::string body = "pong";
    (*deferred)(rpc_status::success, as_bytes(body));
    line.drive();

    REQUIRE(status.has_value());
    REQUIRE(*status == rpc_status::success);
    REQUIRE(response == "pong");
    REQUIRE(line.server.forward_rpc_dropped_count() == 0);
    REQUIRE(line.relay1.procedures().outstanding() == 0);
    REQUIRE(line.relay2.procedures().outstanding() == 0);
}

TEST_CASE("forward_reply_reresolve: a relayed reply with no live route back fails via caller timeout and manufactures no route", "[integration][forward_reply_reresolve]")
{
    relay_line line;

    // The caller reaches the responder via the relay, but the responder is given NO return candidate for
    // the caller — its reply route_selects over an empty candidate span.
    line.caller.ingest_peer_report(line.id_relay, reach_report(line.id_server));
    line.drive();

    bool served = false;
    line.server.procedures().serve("demo.void",
                                   [&](std::span<const std::byte>, rpc_forwarder::reply_fn &reply) {
                                       served = true;
                                       reply(rpc_status::success, {});
                                   });

    std::optional<rpc_status> status;
    line.caller.call(line.id_server, "demo.void", as_bytes(std::string{"q"}),
                     [&](rpc_status s, std::span<const std::byte>) { status = s; });
    line.drive();

    REQUIRE(served);                                            // the request transited the relay
    REQUIRE(line.server.forward_rpc_dropped_count() == 1);      // the reply dropped over empty candidates
    REQUIRE_FALSE(status);                                      // no reply, no route manufactured
    REQUIRE(line.relay.procedures().outstanding() == 0);        // the relay held no correlation

    // The caller's pending call fails through its own deadline, not a forged reply.
    line.advance(std::chrono::hours(2));
    REQUIRE(status.has_value());
    REQUIRE(*status == rpc_status::timeout);
}
