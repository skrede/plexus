#ifndef HPP_GUARD_TESTS_INTEGRATION_PEER_OBSERVER_ASIO_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_PEER_OBSERVER_ASIO_COMMON_H

// The live-TCP peer-observer behavior oracle over asio loopback. A recording observer
// (shared header) is registered on a real routing_engine driven over asio_transport
// with NO hand-dial, and the same lifecycle + readiness matrix the inproc suite forces
// is proven over a real socket — connected/reconnected/disconnected/rejected/dead plus
// the readiness loop (ready once per cycle over the REAL subscribe->ack loop with no
// faked attaches, double-fire prevention across a real reconnect, the premature-ready
// window, the zero-subscribe immediate ready, the accepted-peer edge subset, and
// posted re-entrant delivery). Every edge is POSTED, so each assertion is made AFTER a
// bounded pump reaches the predicate. The behavioral paths loop N>=100 in-body; the
// ctest invocation is re-run >=3 process runs (a live-networking claim is never made
// from one run). The forged-frame underflow case stays inproc-only (it forges a frame).
// The harness mirrors the real-TCP routing oracle.

#include "recording_observer.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using pio::endpoint;
using pio::handshake_fsm_config;
using pio::reconnect_config;
using pio::peer_kind;
using pio::handshake_outcome;
using engine =
        pio::routing_engine<pasio::asio_policy, pasio::asio_transport, std::chrono::steady_clock>;

namespace observer_asio_fixture {

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed, std::uint8_t version = 1,
                                     std::uint8_t compatible = 1)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = version,
                                .version_minor            = 0,
                                .compatible_version_major = compatible,
                                .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000),
                            std::nullopt, std::nullopt};
}

inline reconnect_config bounded_cfg(std::uint32_t max_attempts)
{
    return reconnect_config{std::chrono::milliseconds(20), std::chrono::milliseconds(80),
                            max_attempts, std::nullopt};
}

struct asio_node
{
    ::asio::io_context      &io;
    pasio::asio_transport    transport;
    plexus::log::null_logger sink;
    engine                   eng;

    asio_node(::asio::io_context &shared, std::uint8_t id_seed, bool eager,
              const reconnect_config &redial = forever_cfg(), std::uint8_t compatible = 1)
            : io(shared)
            , transport(shared)
            , eng(transport, shared, make_cfg(id_seed, 1, compatible), k_long_timeout, redial,
                  k_seed, sink, eager)
    {
        eng.listen({"tcp", "127.0.0.1:0"});
    }

    endpoint listen_ep() const { return {"tcp", "127.0.0.1:" + std::to_string(transport.port())}; }
};

inline std::uint16_t reserve_closed_port()
{
    ::asio::io_context    probe_io;
    pasio::asio_transport probe{probe_io};
    probe.listen({"tcp", "127.0.0.1:0"});
    const auto port = probe.port();
    probe.close();
    return port;
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred,
                       std::chrono::seconds bound_secs = std::chrono::seconds(30))
{
    // A GENEROUS wall-clock backstop, not a tight deadline: the happy path exits the
    // moment the predicate holds, so widening the bound only keeps a heavily-contended
    // host from slipping the clock before the (completed-anyway) work is observed. A
    // genuine regression still fails fast on the predicate never coming true.
    auto bound = std::chrono::steady_clock::now() + bound_secs;
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

inline void settle(::asio::io_context       &io,
                   std::chrono::milliseconds window = std::chrono::milliseconds(40))
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

#endif
