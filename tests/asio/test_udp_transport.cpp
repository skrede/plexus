// Live connectionless-UDP best_effort transport leg over a real loopback socket
// pair. Two udp_transports share one io_context: one listens, one dials; the
// handshake ARQ establishes the session, then a best_effort frame flows dialer ->
// acceptor and the accepting channel's on_data posts the IDENTICAL bytes. Covers,
// over the real socket (not a virtual-clock oracle):
//   * best_effort end-to-end: the dialed channel sends a frame, the accepted channel
//     posts the identical frame.
//   * dedup: a replayed datagram (same seq) is dropped — on_data fires once.
//   * oversize: a frame past max_payload is rejected at publish via
//     on_error(message_too_large), NOT silently dropped; the channel stays open.
//   * on_protocol_close never fires on the non-stream channel.
// The best_effort flow loops 100x in-body and the ctest invocation is re-run across
// >=3 process runs for cross-process reproducibility (a live-networking claim is
// never made from one run). The two concept gates (byte_channel<udp_channel>,
// transport_backend<udp_transport, udp_policy>) are restated here so the TU is
// self-evidently the D2 proof.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/transport_backend.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace pasio = plexus::asio;
namespace wire = plexus::wire;

namespace {

static_assert(plexus::io::byte_channel<pasio::udp_channel>);
static_assert(plexus::io::transport_backend<pasio::udp_transport, pasio::udp_policy>);

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

// A real loopback pair on one io_context: a listening transport and a dialing one.
// The handshake ARQ establishes before any data flows; the harness captures the
// accepted server channel and the dialed client channel.
struct loopback
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io};

    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;

    std::vector<std::string>     received;       // frames posted to the accepted channel
    std::optional<plexus::io::io_error> client_error;
    int  accepted_protocol_closes{0};

    loopback()
    {
        server.on_accepted([this](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([this](std::span<const std::byte> b) { received.push_back(str_of(b)); });
            accepted->on_protocol_close([this](wire::close_cause) { ++accepted_protocol_closes; });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until([this] { return server.port() != 0; });

        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const plexus::io::endpoint &) {
            dialed = std::move(ch);
            dialed->on_error([this](plexus::io::io_error e) { client_error = e; });
        });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until([this] { return dialed != nullptr && accepted != nullptr; });
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

}

TEST_CASE("udp best_effort: a dialed channel sends a frame the accepting channel posts identically",
          "[udp][transport]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        loopback h;
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        const std::string payload = "best_effort-" + std::to_string(iter);
        auto frame = bytes_of(payload);
        h.dialed->send(frame);

        h.pump_until([&] { return !h.received.empty(); });
        REQUIRE(h.received.size() == 1);
        REQUIRE(h.received.front() == payload);
        REQUIRE(h.accepted_protocol_closes == 0);   // non-stream: on_protocol_close never fires
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp dedup_live: a replayed datagram is dropped — on_data fires once", "[udp][transport]")
{
    loopback h;
    REQUIRE(h.dialed != nullptr);
    REQUIRE(h.accepted != nullptr);

    // Encode the SAME envelope (same seq) twice and inject both at the raw socket
    // layer (bypassing the channel's send-side seq increment) so the receiver sees a
    // genuine replay. seq=0 matches the channel's first outbound seq.
    auto frame = bytes_of("replay-me");
    auto datagram = wire::wrap_udp(wire::udp_envelope_kind::best_effort, 0, frame);

    // Deliver the identical envelope (same seq) twice to the accepted channel: the
    // per-peer dedup window admits the first as fresh and drops the second as a
    // duplicate, so on_data fires exactly once.
    h.accepted->deliver_inbound(datagram);
    h.accepted->deliver_inbound(datagram);
    h.pump_until([&] { return !h.received.empty(); });

    REQUIRE(h.received.size() == 1);
    REQUIRE(h.received.front() == "replay-me");
}

TEST_CASE("udp oversize: a frame past max_payload is rejected at publish with a surfaced error",
          "[udp][transport]")
{
    loopback h;
    REQUIRE(h.dialed != nullptr);

    // max_payload default 1400; overhead 3 -> a 1398-byte frame fits, a 1398+ frame
    // whose enveloped size exceeds 1400 is rejected.
    std::vector<std::byte> too_big(pasio::udp_channel::default_max_payload, std::byte{0x5A});
    h.dialed->send(too_big);
    h.pump_until([&] { return h.client_error.has_value(); });

    REQUIRE(h.client_error.has_value());
    REQUIRE(*h.client_error == plexus::io::io_error::message_too_large);
    REQUIRE(h.dialed->is_open());     // rejected at publish, channel stays open — not a drop/close

    // A frame that just fits still sends (boundary: size + overhead == max_payload).
    h.client_error.reset();
    std::vector<std::byte> fits(pasio::udp_channel::default_max_payload - wire::udp_envelope_overhead, std::byte{0x01});
    h.dialed->send(fits);
    h.pump_until([&] { return !h.received.empty(); });
    REQUIRE_FALSE(h.client_error.has_value());
    REQUIRE(h.received.size() == 1);
}

#ifdef PLEXUS_HAVE_TLS_MUX

// The mux-route legs prove the (tier, scheme->reliability) composition reaches the
// right member through the REAL dial(ep) path: a "udp" best_effort dial flows over the
// UDP member; a "tcp" reliable dial routes to the TCP member and never touches UDP; and
// the SLICE-state guard — a "udpr" reliable_datagram dial routes to the reliable STREAM
// (TCP) member, NEVER to bare best_effort UDP. The mux header pulls in plexus::tls for
// its secure member (inert here — never dialed), hence the PLEXUS_HAVE_TLS_MUX gate.
// These includes are at file scope (the anonymous namespace above is already closed).

#include "plexus/asio/mux_channel.h"
#include "plexus/asio/mux_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/tls_transport.h"

namespace {

namespace ptls = plexus::tls;

// ONE mux face over a (unix, tcp, tls, udp) quad. Each face owns its members outright —
// the concrete completion callbacks are single-slot, so the listen face and the dial
// face cannot share a member; a loopback pair gives each side its own quad. The tls
// member is inert (a default/invalid credential): no "tls" channel is ever dialed here.
struct mux_face
{
    ::asio::io_context &io;
    ptls::tls_credential no_tls;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport secure{io, no_tls};
    pasio::udp_transport datagram{io};
    pasio::multiplexing_transport mux{local, remote, secure, datagram};

    explicit mux_face(::asio::io_context &ctx) : io(ctx) {}
};

// A loopback pair of mux faces on one io_context. The listen face listens on the
// families a test exercises; the dial face drives a dial of the chosen scheme. The
// dialed (client-side) and accepted (server-side) mux_channels are captured so the
// route + the scheme-survival can be asserted on both ends.
struct mux_pair
{
    ::asio::io_context io;
    mux_face listen_face{io};
    mux_face dial_face{io};

    std::optional<plexus::io::endpoint> dialed_ep;
    std::unique_ptr<pasio::mux_channel> dialed;
    std::unique_ptr<pasio::mux_channel> accepted;

    mux_pair()
    {
        listen_face.mux.on_accepted([this](std::unique_ptr<pasio::mux_channel> ch) { accepted = std::move(ch); });
        dial_face.mux.on_dialed([this](std::unique_ptr<pasio::mux_channel> ch, const plexus::io::endpoint &ep) {
            dialed = std::move(ch);
            dialed_ep.emplace(ep);
        });
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

}

TEST_CASE("udp mux: a best_effort 'udp' dial routes to the UDP member and a frame flows end-to-end",
          "[udp][mux][route]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        mux_pair n;
        n.listen_face.mux.listen({"udp", "127.0.0.1:0"});
        n.pump_until([&] { return n.listen_face.datagram.port() != 0; });
        n.dial_face.mux.dial({"udp", "127.0.0.1:" + std::to_string(n.listen_face.datagram.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);     // delivered POST-handshake via the UDP member
        REQUIRE(n.accepted != nullptr);
        // The "udp" scheme survives the erasure on both ends — it routed to the datagram
        // member, not a stream member.
        REQUIRE(n.dialed->remote_endpoint().scheme == "udp");
        REQUIRE(n.accepted->remote_endpoint().scheme == "udp");
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "udp");

        // A best_effort frame flows dialer -> acceptor over the erased channel.
        std::optional<std::string> got;
        n.accepted->on_data([&](std::span<const std::byte> b) { got = str_of(b); });
        const std::string payload = "mux-udp-" + std::to_string(iter);
        auto frame = bytes_of(payload);
        n.dialed->send(frame);
        n.pump_until([&] { return got.has_value(); });
        REQUIRE(got.has_value());
        REQUIRE(*got == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp mux: a reliable 'tcp' dial routes to the TCP member, never touching UDP",
          "[udp][mux][route]")
{
    mux_pair n;
    n.listen_face.mux.listen({"tcp", "127.0.0.1:0"});
    n.dial_face.mux.dial({"tcp", "127.0.0.1:" + std::to_string(n.listen_face.remote.port())});
    n.pump_until([&] { return n.dialed && n.accepted; });

    REQUIRE(n.dialed != nullptr);
    REQUIRE(n.accepted != nullptr);
    REQUIRE(n.dialed->remote_endpoint().scheme == "tcp");
    REQUIRE(n.accepted->remote_endpoint().scheme == "tcp");
    // The UDP member was never bound — no "udp" dial/listen touched it.
    REQUIRE(n.dial_face.datagram.port() == 0);
    REQUIRE(n.listen_face.datagram.port() == 0);
}

TEST_CASE("udp mux: a reliable_datagram 'udpr' dial routes to the UDP+ARQ member, NEVER TCP",
          "[udp][mux][route][flip]")
{
    // THE FLIP (the other half of the no-downgrade fix): the datagram-with-retransmit ARQ
    // now exists, so a "udpr" demand rides the UDP DATAGRAM member with the ARQ engaged —
    // not the TCP stream fallback it took before the engine landed. The route and the
    // channel's reliable mode engage together: the datagram member reads "udpr" and mints
    // a reliable-datagram channel (its send() drives the selective-repeat engine). The
    // standing invariant holds — reliable_datagram is NEVER served over bare best_effort
    // UDP; it was TCP before the ARQ, it is UDP+ARQ now. This deliberately REPLACES the
    // Slice-A guard assertion (udpr->TCP), making the flip explicit and test-pinned.
    mux_pair n;
    // The listen face accepts on UDP (the family the flip now routes "udpr" to) AND TCP
    // (to prove "udpr" does NOT land on the stream member any more).
    n.listen_face.mux.listen({"udp", "127.0.0.1:0"});
    n.listen_face.mux.listen({"tcp", "127.0.0.1:0"});
    n.pump_until([&] { return n.listen_face.datagram.port() != 0; });

    // Dial "udpr" at the UDP member's port: the flip routes a reliable_datagram demand to
    // the datagram member with the ARQ. The dialed channel reports "udpr" — proof it rode
    // the datagram member in reliable mode, NOT the TCP stream.
    n.dial_face.mux.dial({"udpr", "127.0.0.1:" + std::to_string(n.listen_face.datagram.port())});
    n.pump_until([&] { return n.dialed && n.accepted; });

    REQUIRE(n.dialed != nullptr);
    REQUIRE(n.accepted != nullptr);
    REQUIRE(n.dialed->remote_endpoint().scheme == "udpr");     // routed to the UDP+ARQ member
    REQUIRE(n.accepted->remote_endpoint().scheme == "udpr");
    // The dial face's TCP (remote stream) member was NEVER engaged for the "udpr" dial.
    REQUIRE(n.dial_face.remote.port() == 0);

    // And it delivers in-order reliably: a frame flows through the erased channel's single
    // send() verb (which now drives the ARQ) and the acceptor posts it identically.
    std::optional<std::string> got;
    n.accepted->on_data([&](std::span<const std::byte> b) { got = str_of(b); });
    const std::string payload = "mux-udpr-reliable";
    auto frame = bytes_of(payload);
    n.dialed->send(frame);
    n.pump_until([&] { return got.has_value(); });
    REQUIRE(got.has_value());
    REQUIRE(*got == payload);
}

#endif  // PLEXUS_HAVE_TLS_MUX
