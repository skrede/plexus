#include "test_udp_transport_common.h"

using namespace udp_transport_fixture;

#ifdef PLEXUS_HAVE_TLS_MUX

// The mux-route legs prove the (tier, scheme->reliability) composition reaches the
// right member through the REAL dial(ep) path: a "udp" best_effort dial flows over the
// UDP member; a "tcp" reliable dial routes to the TCP member and never touches UDP; and
// the SLICE-state guard — a "udpr" reliable_datagram dial routes to the reliable STREAM
// (TCP) member, NEVER to bare best_effort UDP. The mux header pulls in plexus::tls for
// its secure member (inert here — never dialed), hence the PLEXUS_HAVE_TLS_MUX gate.

    #include "plexus/asio/all_backends_mux.h"
    #include "plexus/asio/unix_transport.h"

    #include "plexus/io/polymorphic_byte_channel.h"

    #include "plexus/tls/tls_credential.h"
    #include "plexus/tls/tls_transport.h"
    #include "plexus/tls/dtls_transport.h"

namespace {

namespace ptls = plexus::tls;

// ONE mux face over a (unix, tcp, tls, udp, dtls) quintet. Each face owns its members
// outright — the concrete completion callbacks are single-slot, so the listen face and the
// dial face cannot share a member; a loopback pair gives each side its own quintet. The tls
// and dtls members are inert (a default/invalid credential): no "tls"/"dtls" channel is ever
// dialed here.
struct mux_face
{
    ::asio::io_context &io;
    ptls::tls_credential no_tls;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport secure{io, no_tls};
    pasio::udp_transport datagram{io};
    ptls::dtls_transport secure_datagram{io, no_tls};
    pasio::all_backends_mux mux{local, remote, secure, datagram, secure_datagram};

    explicit mux_face(::asio::io_context &ctx)
            : io(ctx)
    {
    }
};

// A loopback pair of mux faces on one io_context. The listen face listens on the
// families a test exercises; the dial face drives a dial of the chosen scheme. The
// dialed (client-side) and accepted (server-side) polymorphic_byte_channels are captured so the
// route + the scheme-survival can be asserted on both ends.
struct mux_pair
{
    ::asio::io_context io;
    mux_face listen_face{io};
    mux_face dial_face{io};

    std::optional<plexus::io::endpoint> dialed_ep;
    std::unique_ptr<pio::polymorphic_byte_channel> dialed;
    std::unique_ptr<pio::polymorphic_byte_channel> accepted;

    mux_pair()
    {
        listen_face.mux.on_accepted([this](std::unique_ptr<pio::polymorphic_byte_channel> ch) { accepted = std::move(ch); });
        dial_face.mux.on_dialed(
                [this](std::unique_ptr<pio::polymorphic_byte_channel> ch, const plexus::io::endpoint &ep)
                {
                    dialed = std::move(ch);
                    dialed_ep.emplace(ep);
                });
    }

    template<typename Pred>
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

TEST_CASE("udp mux: a best_effort 'udp' dial routes to the UDP member and a frame flows end-to-end", "[udp][mux][route]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        mux_pair n;
        n.listen_face.mux.listen({"udp", "127.0.0.1:0"});
        n.pump_until([&] { return n.listen_face.datagram.port() != 0; });
        n.dial_face.mux.dial({"udp", "127.0.0.1:" + std::to_string(n.listen_face.datagram.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr); // delivered POST-handshake via the UDP member
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
        auto frame                = bytes_of(payload);
        n.dialed->send(frame);
        n.pump_until([&] { return got.has_value(); });
        REQUIRE(got.has_value());
        REQUIRE(*got == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp mux: a reliable 'tcp' dial routes to the TCP member, never touching UDP", "[udp][mux][route]")
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

TEST_CASE("udp mux: a reliable_datagram 'udpr' dial routes to the UDP+ARQ member, NEVER TCP", "[udp][mux][route][flip]")
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
    REQUIRE(n.dialed->remote_endpoint().scheme == "udpr"); // routed to the UDP+ARQ member
    REQUIRE(n.accepted->remote_endpoint().scheme == "udpr");
    // The dial face's TCP (remote stream) member was NEVER engaged for the "udpr" dial.
    REQUIRE(n.dial_face.remote.port() == 0);

    // And it delivers in-order reliably: a frame flows through the erased channel's single
    // send() verb (which now drives the ARQ) and the acceptor posts it identically.
    std::optional<std::string> got;
    n.accepted->on_data([&](std::span<const std::byte> b) { got = str_of(b); });
    const std::string payload = "mux-udpr-reliable";
    auto frame                = bytes_of(payload);
    n.dialed->send(frame);
    n.pump_until([&] { return got.has_value(); });
    REQUIRE(got.has_value());
    REQUIRE(*got == payload);
}

#endif // PLEXUS_HAVE_TLS_MUX
