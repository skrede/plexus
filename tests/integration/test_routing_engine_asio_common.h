#ifndef HPP_GUARD_TESTS_INTEGRATION_ROUTING_ENGINE_ASIO_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_ROUTING_ENGINE_ASIO_COMMON_H

// Gated real-TCP routing oracle over asio loopback. It drives the routing_engine
// over asio_transport with NO hand-dial — the engine itself opens the connection,
// never a manual connect on a raw socket, never a spin on a server-channel handle:
// the engine's reach -> driver.start -> transport.dial -> on_dialed ->
// registry build-from-record -> start() tail runs over real TCP. It proves BOTH
// dial knobs plexus<->plexus on the wall clock:
//   - LAZY (default): note_peer records awareness and opens NO connection; only a
//     demand call (reach/subscribe) dials, then the handshake completes and a real
//     published message carrying the minted epoch flows;
//   - EAGER (opt-in): note_peer ALONE dials+handshakes with no demand call.
// It ALSO proves the receive-path source-peer identity leg over the wire: one node
// connected to TWO distinct remote peers resolves each delivered frame to its OWN
// source-peer identity in the per-session sinks — peer-X's message lands attributed
// to X and peer-Y's to Y, no cross-attribution. That two-peer leg dials both peers
// near-simultaneously (overlapping in-flight dials whose TCP connects can complete
// out of order), so it is also the adversary for the dial-completion correlation:
// the engine must route each completed channel back to ITS slot by the dial
// endpoint, not by arrival order. The behavioral happy paths loop N>=100 in-body;
// the ctest invocation is re-run >=3 process runs (a live-networking claim is never
// made from one run).

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
using engine = pio::routing_engine<pasio::asio_policy, pasio::asio_transport, std::chrono::steady_clock>;

namespace routing_asio_fixture {

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

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
}

// One asio engine on a shared io_context, listening on an ephemeral port. Member
// ORDER: io_context/transport BEFORE the engine so destruction unwinds the engine's
// channels/sessions before the io_context they borrow.
struct asio_node
{
    ::asio::io_context      &io;
    pasio::asio_transport    transport;
    plexus::log::null_logger sink;
    engine                   eng;

    // listen_now=false defers the listen so the caller can bring the acceptor up
    // LATE (the refused-then-up path that forces an out-of-order dial completion).
    asio_node(::asio::io_context &shared, std::uint8_t id_seed, bool eager, bool listen_now = true)
            : io(shared)
            , transport(shared)
            , eng(transport, shared, make_cfg(id_seed), k_long_timeout, forever_cfg(), k_seed, sink, eager)
    {
        if(listen_now)
            eng.listen({"tcp", "127.0.0.1:0"});
    }

    void listen_on(std::uint16_t port)
    {
        eng.listen({"tcp", "127.0.0.1:" + std::to_string(port)});
    }
    endpoint listen_ep() const
    {
        return {"tcp", "127.0.0.1:" + std::to_string(transport.port())};
    }
};

// Reserve a free loopback port, then close it so a dial there is refused until a
// listener rebinds it (reuse_address is set on the acceptor).
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
inline void pump_until(::asio::io_context &io, Pred pred)
{
    // A GENEROUS wall-clock backstop, not a tight deadline: the happy path exits the
    // instant the predicate holds, so a wide bound only keeps a contended host from
    // slipping the clock before the (completed-anyway) work is observed; a real
    // regression still fails on the predicate never coming true.
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

inline void settle(::asio::io_context &io, std::chrono::milliseconds window = std::chrono::milliseconds(30))
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

#endif
