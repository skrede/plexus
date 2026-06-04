#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace wire = plexus::wire;
namespace pio = plexus::io;

using plexus::io::handshake_fsm_config;
using session = pio::peer_session<pasio::asio_policy>;
using msg_forwarder = pio::message_forwarder<pasio::asio_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::asio_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

// Synthesize a unidirectional data frame for "topic" carrying a chosen session_id,
// exactly as the forwarder frames one — built over an inproc capture link so the
// bytes are produced by the production framing path, then handed verbatim to a
// receiver's on_receive (the framing is backend-independent; on_receive consumes
// raw frame bytes). Handing this to the asio receiver exercises the real staleness
// gate, not a hand-strip.
std::vector<std::byte> make_data_frame(const std::string &payload, std::uint8_t session_id)
{
    using plexus::inproc::inproc_bus;
    using plexus::inproc::inproc_executor;
    using plexus::inproc::inproc_channel;
    using inproc_msg = pio::message_forwarder<plexus::inproc::inproc_policy>;

    inproc_msg framer;
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> capture(ex);
    inproc_channel<> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    inproc_msg::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

// One bidirectional TCP loopback link turned into a peer_session pair, stood up
// entirely through the transport's listen/dial rendezvous (no hand-dial, no manual
// socket().connect / poll loop). The dialer end becomes the requester
// (is_inbound_bootstrap=false) and the accepted end becomes the responder
// (is_inbound_bootstrap=true) — the realistic asymmetric handshake the B1 bridge
// completes on both sides. The sessions are deferred in std::optional, built only
// once on_dialed/on_accepted deliver the channels. A real published message rides
// the SAME live TCP channels the handshake established. Each instance stands up a
// fresh transport + io_context so the looped iterations are independent.
struct tcp_link
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};

    msg_forwarder req_messages;
    msg_forwarder resp_messages;
    rpc_forwarder req_procedures{io, k_long_timeout};
    rpc_forwarder resp_procedures{io, k_long_timeout};

    plexus::io::peer_context<pasio::asio_policy> req_ctx;   // the dialer slot's per-peer record
    plexus::io::peer_context<pasio::asio_policy> resp_ctx;  // the accepted slot's per-peer record
    std::optional<session> requester;   // the dialer (client) end
    std::optional<session> responder;   // the accepted (server) end

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    explicit tcp_link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted([this, timeout](std::unique_ptr<pasio::asio_channel> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            responder.emplace(resp_ctx, io, make_cfg(0x01), timeout,
                              resp_messages, resp_procedures, true);
            responder->on_message([this](std::string_view, std::span<const std::byte> d) {
                resp_received.emplace_back(to_string(d));
            });
            responder->start();
        });
        transport.on_dialed([this, timeout](std::unique_ptr<pasio::asio_channel> ch, const pio::endpoint &) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            requester.emplace(req_ctx, io, make_cfg(0x02), timeout,
                              req_messages, req_procedures, false);
            requester->on_message([this](std::string_view, std::span<const std::byte> d) {
                req_received.emplace_back(to_string(d));
            });
            requester->start();
        });

        transport.listen({"tcp", "127.0.0.1:0"});
        transport.dial({"tcp", "127.0.0.1:" + std::to_string(transport.port())});
    }

    // Pump until pred() or a generous bounded wall-clock deadline, so a regression
    // (a handshake or delivery that never lands) fails fast rather than hanging.
    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    // Drain the io_context for a short bounded window — used to prove the ABSENCE of
    // an event (a dropped frame never delivers) and to settle the subscribe handshake,
    // where there is no positive predicate to wait on. A loopback round-trip lands in
    // microseconds, so a short drained window is ample without ever hanging.
    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

}

TEST_CASE("asio peer_session pair completes the handshake over real TCP and mints epochs, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });

        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("asio peer_session: a dialed (one-directional) connection completes BOTH sides over real TCP — the accepted bootstrap responder answers, the dialer mints off the response, gated data flows both ways, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string downward = "dialer-to-responder-over-tcp";
    const std::string upward = "responder-to-dialer-over-tcp";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;   // the dial rendezvous: only the dialer dials, the accepted end bootstraps
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });

        // Both complete over real TCP without a simultaneous connect: the accepted
        // bootstrap responder sent its accept response over the socket, so the dialer
        // completed and minted its own epoch.
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach(l.requester->msg_peer(), "up"));
        REQUIRE(l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "up"));
        l.settle();

        l.req_messages.publish("down", as_bytes(downward), l.requester->session_id());
        l.resp_messages.publish("up", as_bytes(upward), l.responder->session_id());
        l.pump_until([&] { return !l.resp_received.empty() && !l.req_received.empty(); });

        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == downward);
        REQUIRE(l.req_received.size() == 1);
        REQUIRE(l.req_received[0] == upward);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        REQUIRE(l.requester->peer_session_id() == l.responder->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("asio peer_session: a real published message flows post-handshake over TCP and latches the epoch, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload = "real-published-bytes-over-tcp";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        // The responder subscribes (so its forwarder resolves the topic_hash on the
        // receive tail); the requester fans the topic toward its peer, then publishes
        // carrying its minted epoch. The frame rides the live TCP channel.
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle();   // drain the subscribe handshake
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());

        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("asio peer_session: the data-path staleness gate FIRES over TCP — a mismatched epoch is dropped, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string good = "latched-epoch-bytes";
    const std::string stale = "stale-session-bytes";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle();

        // A real publish over TCP latches the requester's epoch on the responder.
        l.req_messages.publish("topic", as_bytes(good), l.requester->session_id());
        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(l.resp_received.size() == 1);
        const auto latched = l.responder->peer_session_id();
        REQUIRE(latched == l.requester->session_id());

        // A frame for the SAME topic carrying a DIFFERENT non-zero epoch is dropped:
        // the sink does not grow. Fed through the production receive path. on_receive
        // gates synchronously (no post), so the drop is observable immediately — a
        // wall-clock wait would only risk a false green under load.
        const std::uint8_t stale_epoch = static_cast<std::uint8_t>(latched == 200 ? 199 : 200);
        auto stale_frame = make_data_frame(stale, stale_epoch);
        l.responder->on_receive(stale_frame);
        REQUIRE(l.resp_received.size() == 1);   // DROPPED, not delivered

        // A frame carrying the latched epoch IS delivered.
        auto fresh_frame = make_data_frame(good, latched);
        l.responder->on_receive(fresh_frame);
        l.pump_until([&] { return l.resp_received.size() >= 2; });
        REQUIRE(l.resp_received.size() == 2);
        REQUIRE(l.resp_received[1] == good);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
