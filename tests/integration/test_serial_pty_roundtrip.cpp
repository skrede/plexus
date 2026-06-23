#include "test_serial_common.h"

#include "plexus/io/frame_router.h"

#include "plexus/wire/data_frame.h"

#include <catch2/catch_test_macros.hpp>

using namespace serial_fixture;

// A complete framed message round-trips host<->host over an openpty pair with NO hardware.
// One serial_channel adopts the master fd, the other the slave fd, on one io_context. A
// production-framed unidirectional message is sent over one channel; the other channel's on_data
// (which re-frames each reassembled frame back to header-on bytes per the byte_channel contract)
// delivers it; a frame_router strips the header and the inner payload decodes to the exact bytes.
// Looped N>=50 in-body; the ctest invocation is re-run >=3 process runs (a serial round-trip claim
// is never made from one run).
TEST_CASE("serial channel: a framed message round-trips over an openpty pair, looped",
          "[integration][serial][roundtrip]")
{
    constexpr int     k_iterations = 50;
    const std::string payload      = "plexus-serial-pty-payload";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair           pty;
        ::asio::io_context io;

        auto tx = adopt_channel(io, pty.take_master());
        auto rx = adopt_channel(io, pty.take_slave());

        std::optional<std::string> received;
        plexus::log::null_logger    router_sink;
        pio::frame_router           router{router_sink};
        router.on_unidirectional(
                [&](const wire::frame_header &, std::span<const std::byte> inner)
                {
                    auto decoded = wire::decode_unidirectional(inner);
                    if(decoded)
                        received = to_string(decoded->data);
                });
        rx->on_data([&](std::span<const std::byte> frame) { router.route(frame); });

        const auto frame = make_data_frame(payload, /*session_id=*/1);
        tx->send(frame);

        pump_until(io, [&] { return received.has_value(); });

        REQUIRE(received.has_value());
        REQUIRE(*received == payload); // the exact opaque bytes, every iteration
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

// The point-at-port bring-up handshake completes over the framed serial link, driving the
// REUSED handshake_fsm via the peer_session bridge (no FSM change). The pty pair is the only peer:
// the requester end (the master fd, is_inbound_bootstrap=false) drives the outbound handshake
// request, the responder end (the slave fd, is_inbound_bootstrap=true) takes the inbound bootstrap
// and answers — the same asymmetric handshake the socket path completes, here over the serial
// wire. Both ends reach is_complete() and mint a non-zero session_id. Looped N>=50 in-body; re-run
// >=3 process runs.
TEST_CASE("serial channel: a peer_session pair completes the point-at-port handshake over an "
          "openpty pair and mints epochs, looped",
          "[integration][serial][handshake]")
{
    constexpr int k_iterations = 50;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair           pty;
        ::asio::io_context io;

        plexus::log::null_logger sink;
        serial_msg_fwd req_messages{sink};
        serial_msg_fwd resp_messages{sink};
        serial_rpc_fwd req_procedures{io, k_long_timeout, sink};
        serial_rpc_fwd resp_procedures{io, k_long_timeout, sink};

        pio::peer_context<pasio::serial_policy> req_ctx;  // the dialer (requester) slot
        pio::peer_context<pasio::serial_policy> resp_ctx; // the listener (responder) bootstrap slot

        req_ctx.channel    = adopt_channel(io, pty.take_master());
        req_ctx.node_name  = "responder-node";
        resp_ctx.channel   = adopt_channel(io, pty.take_slave());
        resp_ctx.node_name = "requester-node";

        serial_session requester{req_ctx,  io, make_cfg(0x02), k_long_timeout,
                                 req_messages, req_procedures, /*is_inbound_bootstrap=*/false, sink};
        serial_session responder{resp_ctx, io, make_cfg(0x01), k_long_timeout,
                                  resp_messages, resp_procedures, /*is_inbound_bootstrap=*/true,
                                  sink};
        requester.start();
        responder.start();

        pump_until(io, [&] { return requester.is_complete() && responder.is_complete(); });

        REQUIRE(requester.is_complete());
        REQUIRE(responder.is_complete());
        REQUIRE(requester.session_id() != 0);
        REQUIRE(responder.session_id() != 0);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}
