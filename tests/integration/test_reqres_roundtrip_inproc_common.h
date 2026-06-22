#ifndef HPP_GUARD_TESTS_INTEGRATION_REQRES_ROUNDTRIP_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_REQRES_ROUNDTRIP_INPROC_COMMON_H

#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using procedure_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::wire::rpc_status;

namespace reqres_inproc_fixture {

// Far past any clock advance these roundtrip/alloc cases perform — they drain
// deterministically and never move the clock, so the per-call deadline never trips.
constexpr auto k_long_deadline = std::chrono::hours(1);

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A bidirectional inproc RPC link: a caller forwarder and a provider forwarder,
// each receiving its inbound frames through a frame_router (the router owns the
// frame_header.type switch; the forwarder stays a pure consumer of pre-demuxed
// inner payloads). Caller requests ride caller_tx -> provider_rx; provider
// replies ride provider_tx -> caller_rx. drive() drains the step-loop to
// quiescence (deterministic — no wall clock, no polling).
struct rpc_link
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};

    inproc_channel<> caller_tx{ex};
    inproc_channel<> caller_rx{ex};
    inproc_channel<> provider_tx{ex};
    inproc_channel<> provider_rx{ex};

    plexus::log::null_logger sink;

    procedure_forwarder caller{ex, k_long_deadline, sink};
    procedure_forwarder provider{ex, k_long_deadline, sink};

    plexus::io::frame_router caller_router;
    plexus::io::frame_router provider_router;

    procedure_forwarder::peer caller_peer{caller_tx, "provider-node"};
    procedure_forwarder::peer provider_peer{provider_tx, "caller-node"};

    rpc_link() { wire(); }

    void wire()
    {
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner)
                                       { provider.deliver_request(provider_peer, inner); });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner)
                                      { caller.deliver_response(caller_peer, inner); });

        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive() { ex.drain(); }
};

}

#endif
