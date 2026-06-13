// Gated real-TCP N>=3-peer concurrent-drop oracle over asio loopback. One dialer
// engine (routing_engine over asio_transport) holds N established peer sessions; the
// engine's OWN production drop route detects each drop and re-dials — no harness
// on_error wiring, no hand-injected established-drop call. Several established sessions
// drop AT ONCE by closing the accepted (responder) sockets back-to-back; the dialer's read
// loops surface the FIN -> on_error -> each slot's driver re-dials INDEPENDENTLY, and
// every survivor stays complete and is never re-dialed (its attempt_count unchanged).
// A surrender-without-collateral leg arms a small max_attempts on the dialer and drops
// ONE peer past the bound: is_dead(that id) while the others stay is_connected. Every
// drop is a REAL socket().close() on the accepted end (Pitfall 1: close the accepted,
// not the dialer's own end). The behavioral paths loop in-body; the ctest invocation
// is re-run >=3 process runs (a live-networking claim is never made from one run).
// Links plexus::inproc for the forwarder framing path the peers reuse.

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/routing_engine.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

using pio::endpoint;
using pio::handshake_fsm_config;
using pio::reconnect_config;
using engine = pio::routing_engine<pasio::asio_policy, pasio::asio_transport, std::chrono::steady_clock>;
using session = pio::peer_session<pasio::asio_policy>;
using msg_forwarder = pio::message_forwarder<pasio::asio_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::asio_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config fast_cfg()
{
    return reconnect_config{std::chrono::milliseconds(5), std::chrono::milliseconds(50),
                            std::nullopt, std::nullopt};
}

reconnect_config bounded_cfg(std::uint32_t max_attempts)
{
    return reconnect_config{std::chrono::milliseconds(5), std::chrono::milliseconds(50),
                            max_attempts, std::nullopt};
}

// One peer node on the shared io_context: its OWN asio_transport listening on an
// ephemeral loopback port, and an inbound-bootstrap responder session rebuilt on each
// accept (the dialer re-accepts after every re-dial). It RETAINS the live accepted
// channel so the test can close its socket directly — the injection-free real drop
// verb. Member ORDER: transport BEFORE the optional session so destruction unwinds the
// session before the transport.
struct peer_node
{
    pasio::asio_transport transport;
    msg_forwarder messages;
    rpc_forwarder procedures;
    pio::peer_context<pasio::asio_policy> ctx;
    std::optional<session> responder;
    pasio::asio_channel *accepted{nullptr};
    plexus::node_id id;
    endpoint ep;

    peer_node(::asio::io_context &io, std::uint8_t seed)
        : transport(io)
        , messages()
        , procedures(io, k_long_timeout)
        , id(make_id(seed))
    {
        transport.on_accepted([this, &io, seed](std::unique_ptr<pasio::asio_channel> ch) {
            accepted = ch.get();
            ctx.channel = std::move(ch);
            ctx.node_name = "dialer-node";
            responder.emplace(ctx, io, make_cfg(seed), k_long_timeout,
                              messages, procedures, true);
            responder->start();
        });
        transport.listen({"tcp", "127.0.0.1:0"});
        ep = {"tcp", "127.0.0.1:" + std::to_string(transport.port())};
    }

    // Close the accepted (responder) socket so the dialer's read loop sees the FIN —
    // the production drop route on the dialer's slot re-dials. Closing the accepted end
    // (not the dialer's own) is what surfaces the drop on the dialer (Pitfall 1).
    void drop() { if(accepted) accepted->socket().close(); }
};

// The dialer engine plus its N peers on one io_context. The engine reaches every peer
// to a complete session. The engine's redial config is selectable so the surrender
// leg can arm a bounded dialer. Member ORDER: io_context/transport BEFORE the engine.
struct multipeer_net
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};
    engine a;
    std::vector<std::unique_ptr<peer_node>> peers;

    multipeer_net(std::size_t n, const reconnect_config &a_redial = fast_cfg())
        : a(transport, io, make_cfg(0xA1), k_long_timeout, a_redial, k_seed, false)
    {
        a.listen({"tcp", "127.0.0.1:0"});
        for(std::size_t i = 0; i < n; ++i)
        {
            auto seed = static_cast<std::uint8_t>(0xB0 + i);
            peers.push_back(std::make_unique<peer_node>(io, seed));
            a.note_peer(peers.back()->id, peers.back()->ep);
            a.reach(peers.back()->id);
        }
    }

    peer_node &peer(std::size_t i) { return *peers[i]; }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        // A GENEROUS wall-clock backstop, not a tight deadline: the happy path exits the
        // instant the predicate holds, so a wide bound only keeps a contended host from
        // slipping the clock before the (completed-anyway) work is observed; a real
        // regression still fails on the predicate never coming true.
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(30))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    bool all_connected()
    {
        for(auto &p : peers)
            if(!a.is_connected(p->id))
                return false;
        return true;
    }
};

}

TEST_CASE("multipeer asio: concurrent real socket closes re-dial each dropped slot independently; survivors are undisturbed over real TCP",
          "[integration][multipeer][asio]")
{
    constexpr int k_iterations = 30;
    constexpr std::size_t k_n = 3;
    constexpr std::size_t k_dropped = 2;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        multipeer_net net(k_n);
        net.pump_until([&] { return net.all_connected(); });
        REQUIRE(net.all_connected());

        std::array<std::uint64_t, k_n> epoch{};
        std::array<std::uint32_t, k_n> attempts{};
        for(std::size_t i = 0; i < k_n; ++i)
        {
            epoch[i] = net.a.session_for(net.peer(i).id)->session_id();
            attempts[i] = net.a.attempt_count(net.peer(i).id);
        }

        // Concurrent drop: close K accepted sockets back-to-back. The dialer's read
        // loops surface each FIN -> on_error -> the slot's driver re-dials (production
        // route; no hand-injected established-drop call).
        for(std::size_t i = 0; i < k_dropped; ++i)
            net.peer(i).drop();

        // Each dropped slot re-handshakes a FRESH epoch on its own backoff.
        net.pump_until([&] {
            for(std::size_t i = 0; i < k_dropped; ++i)
                if(!(net.a.is_connected(net.peer(i).id)
                     && net.a.session_for(net.peer(i).id)->session_id() != epoch[i]))
                    return false;
            return true;
        });
        net.settle();

        for(std::size_t i = 0; i < k_dropped; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() != epoch[i]);
            REQUIRE(net.a.attempt_count(net.peer(i).id) >= attempts[i] + 1);
        }
        // Survivors: never dropped, so never re-dialed — epoch and attempt_count hold.
        for(std::size_t i = k_dropped; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() == epoch[i]);
            REQUIRE(net.a.attempt_count(net.peer(i).id) == attempts[i]);
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("multipeer asio: one peer crossing a surrender bound is is_dead while every live peer stays connected over real TCP",
          "[integration][multipeer][asio]")
{
    constexpr int k_iterations = 20;
    constexpr std::size_t k_n = 3;
    constexpr std::uint32_t k_max_attempts = 3;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        multipeer_net net(k_n, bounded_cfg(k_max_attempts));
        net.pump_until([&] { return net.all_connected(); });
        REQUIRE(net.all_connected());

        // Surrender peer 0: stop its listener so every re-dial is refused, then close
        // its current accepted socket to start the backoff. Each refused re-dial
        // advances slot 0's attempt_count until it crosses the bound and is_dead latches.
        net.peer(0).transport.close();
        net.peer(0).drop();
        net.pump_until([&] { return net.a.is_dead(net.peer(0).id); });
        REQUIRE(net.a.is_dead(net.peer(0).id));

        // Surrender without collateral: every other peer stays connected and not dead.
        net.settle();
        for(std::size_t i = 1; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(!net.a.is_dead(net.peer(i).id));
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
