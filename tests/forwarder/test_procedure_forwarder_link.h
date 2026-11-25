#ifndef HPP_GUARD_TESTS_FORWARDER_TEST_PROCEDURE_FORWARDER_LINK_H
#define HPP_GUARD_TESTS_FORWARDER_TEST_PROCEDURE_FORWARDER_LINK_H

#include "test_procedure_forwarder_common.h"

#include "plexus/io/frame_router.h"

#include "plexus/log/logger.h"

#include <span>
#include <cstddef>

namespace procedure_forwarder_fixture {

// A bidirectional inproc RPC link: a caller forwarder and a provider forwarder,
// each with its own receive sink, cross-wired so the caller's rpc_request reaches
// the provider's deliver_request and the provider's rpc_response reaches the
// caller's deliver_response. Each side demuxes its inbound frames through a
// frame_router. drive() drains the step-loop.
struct rpc_link
{
    inproc_bus<> bus;
    inproc_executor<> ex;

    inproc_channel<> caller_tx;
    inproc_channel<> caller_rx;
    inproc_channel<> provider_tx;
    inproc_channel<> provider_rx;

    plexus::log::null_logger sink;

    procedure_forwarder caller;
    procedure_forwarder provider;

    plexus::io::frame_router caller_router;
    plexus::io::frame_router provider_router;

    procedure_forwarder::peer caller_peer;
    procedure_forwarder::peer provider_peer;

    explicit rpc_link(plexus::log::logger &caller_log)
            : ex{bus}
            , caller_tx{ex}
            , caller_rx{ex}
            , provider_tx{ex}
            , provider_rx{ex}
            , caller(ex, k_long_deadline, caller_log)
            , provider{ex, k_long_deadline, sink}
            , caller_router(sink)
            , provider_router(sink)
            , caller_peer{caller_tx, "provider-node"}
            , provider_peer{provider_tx, "caller-node"}
    {
        wire();
    }

    rpc_link()
            : ex{bus}
            , caller_tx{ex}
            , caller_rx{ex}
            , provider_tx{ex}
            , provider_rx{ex}
            , caller{ex, k_long_deadline, sink}
            , provider{ex, k_long_deadline, sink}
            , caller_router(sink)
            , provider_router(sink)
            , caller_peer{caller_tx, "provider-node"}
            , provider_peer{provider_tx, "caller-node"}
    {
        wire();
    }

    void wire()
    {
        // Caller requests ride caller_tx -> provider_rx; provider replies ride
        // provider_tx -> caller_rx.
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner) { provider.deliver_request(provider_peer, inner); });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner) { caller.deliver_response(caller_peer, inner); });

        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive()
    {
        ex.drain();
    }
};

}

#endif
