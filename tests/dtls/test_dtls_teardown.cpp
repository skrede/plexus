#include "dtls_test_support.h"

#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/detail/asio_inbound_demux.h"

#include <catch2/catch_test_macros.hpp>

#include <openssl/ssl.h>
#include <openssl/bio.h>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pasio = plexus::asio;
namespace pdetail = plexus::asio::detail;

// Teardown-discipline regressions for the DTLS channel/demux lifetime defects:
//   * the close_notify branch must POST on_closed off its own drain_inbound stack
//     (a consumer that destroys the channel in on_closed must not free `this`
//     mid-drain — a UAF caught under asan);
//   * the inbound demux must OVERWRITE a stale entry on a re-insert (a re-dial to a
//     dest that already has an entry must route to the NEW channel, not the old).

namespace {

// Drive a real server dtls_channel to a completed mutual handshake against a RAW
// OpenSSL DTLS client built on the same shared credential CTX, then emit a clean
// close_notify from the raw client and feed it to the server channel. This is the
// only way to reach the SSL_ERROR_ZERO_RETURN branch — the channel has no graceful
// shutdown verb, so a real peer's close_notify must drive it.
struct close_notify_harness
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    plexus::io::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    pasio::udp_server server_sock{io};
    pasio::udp_server client_sock{io};         // a REAL client socket: the server sends here

    std::unique_ptr<ptls::dtls_channel> server_ch;

    SSL *client_ssl{nullptr};
    BIO *client_ext{nullptr};                 // the client's external BIO (wire side)

    std::vector<std::vector<std::byte>> server_to_client;   // datagrams the server emitted
    bool server_complete{false};

    close_notify_harness(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        // The server channel sends to the client's REAL bound socket, whose
        // on_datagram captures the server flights to feed into the raw client SSL.
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred,
                                                         server_cookie, ptls::dtls_channel::role::server);
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            server_to_client.emplace_back(b.begin(), b.end());
        });
        server_ch->on_external_complete([this] { server_complete = true; });

        build_client();
    }

    close_notify_harness(const close_notify_harness &) = delete;
    close_notify_harness &operator=(const close_notify_harness &) = delete;

    ~close_notify_harness()
    {
        if(client_ssl)
            SSL_free(client_ssl);             // frees the internal BIO; we free the external
        if(client_ext)
            BIO_free(client_ext);
    }

    void build_client()
    {
        client_ssl = SSL_new(&client_cred.ssl_ctx());
        REQUIRE(client_ssl != nullptr);
        BIO *internal = nullptr;
        REQUIRE(BIO_new_bio_pair(&internal, 4096, &client_ext, 4096) == 1);
        SSL_set_bio(client_ssl, internal, internal);
        SSL_set_mtu(client_ssl, static_cast<long>(ptls::dtls_channel::default_max_payload));
        SSL_set_options(client_ssl, SSL_OP_NO_QUERY_MTU);
        SSL_set_connect_state(client_ssl);
    }

    // Pull every pending record off the client's external BIO and feed it to the
    // server channel's deliver_inbound (the wire hop, client -> server).
    void client_flush_to_server()
    {
        std::array<unsigned char, 2048> buf{};
        while(true)
        {
            const int n = BIO_read(client_ext, buf.data(), static_cast<int>(buf.size()));
            if(n <= 0)
                break;
            server_ch->deliver_inbound(std::span<const std::byte>{
                reinterpret_cast<const std::byte *>(buf.data()), static_cast<std::size_t>(n)});
        }
    }

    // Feed every datagram the server emitted into the client's external BIO.
    void server_flush_to_client()
    {
        auto sc = std::move(server_to_client);
        server_to_client.clear();
        for(auto &dg : sc)
            BIO_write(client_ext, dg.data(), static_cast<int>(dg.size()));
    }

    // Run the handshake to completion: drive the client, relay both directions,
    // pump the io_context (the server channel posts on its executor).
    void complete_handshake()
    {
        server_ch->start_handshake();
        auto bound = std::chrono::steady_clock::now() + std::chrono::milliseconds{6000};
        while((!server_complete || !SSL_is_init_finished(client_ssl))
              && std::chrono::steady_clock::now() < bound)
        {
            const int r = SSL_do_handshake(client_ssl);
            if(r <= 0)
            {
                const int e = SSL_get_error(client_ssl, r);
                if(e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE)
                    FAIL("client handshake error " << e);
            }
            client_flush_to_server();
            io.poll();
            if(io.stopped())
                io.restart();
            server_flush_to_client();
        }
    }

    // Emit a clean DTLS close_notify from the client and deliver it to the server.
    void client_send_close_notify()
    {
        (void)SSL_shutdown(client_ssl);
        client_flush_to_server();
    }
};

// A real-transport dialed loopback: a server-side and a client-side dtls_transport
// (each cross-pinning the other) over one io_context. The accepted/dialed channels
// land via on_accepted/on_dialed; an app frame from the dialer travels over the wire
// to the server transport's on_datagram -> demux lookup -> deliver_inbound — the exact
// path the engine-teardown UAF lives on (the raw OpenSSL harness above drives
// deliver_inbound directly and so bypasses the demux).
struct dial_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    bool dial_failed{false};

    dial_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
        , server(io, server_cred)
        , client(io, client_cred)
    {
        server.on_accepted([this](std::unique_ptr<ptls::dtls_channel> ch) { accepted = std::move(ch); });
        client.on_dialed([this](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &) { dialed = std::move(ch); });
        client.on_dial_failed([this](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        client.dial({"dtls", "127.0.0.1:" + std::to_string(server.port())});
    }

    void pump_until_ready()
    {
        pdt::pump_until(io, [this] { return (accepted && dialed) || dial_failed; });
    }

    void drain()
    {
        for(int i = 0; i < 256; ++i)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

}

TEST_CASE("dtls.teardown: an inbound datagram after the engine destroys a completed accepted channel is safe, looped",
          "[dtls][teardown]")
{
    pdt::identity_fixture server_id("td_srv");
    pdt::identity_fixture client_id("td_cli");

    // Complete the dialed handshake, then destroy the engine-owned accepted channel
    // mid-session and have the dialer send another frame from the SAME source. Pre-fix:
    // the server transport's demux still holds the freed accepted-channel pointer, so
    // on_datagram -> lookup -> deliver_inbound is a heap-use-after-free (asan). The fix
    // erases the demux entry when the channel tears down, so the frame is a clean MISS.
    constexpr int k_iterations = 50;
    int survived = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        dial_link l(server_id, client_id);
        l.pump_until_ready();
        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr);

        // Engine teardown: destroy the completed accepted channel while the dialer (and
        // the demux entry keyed on its source endpoint) stay live.
        l.accepted.reset();
        l.drain();

        // The dialer sends an app frame over the live DTLS channel: it arrives at the
        // server from the SAME source the demux is keyed on. The torn-down entry must be
        // gone, never a freed-pointer deref.
        const std::string payload = "after-teardown-" + std::to_string(i);
        std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(payload.data()),
                                     reinterpret_cast<const std::byte *>(payload.data()) + payload.size());
        l.dialed->send(std::span<const std::byte>{frame});
        l.drain();

        ++survived;   // reaching here without an asan UAF abort is the proof
    }
    REQUIRE(survived == k_iterations);
}

TEST_CASE("dtls.teardown: a consumer that destroys the channel in on_closed survives the close_notify, looped",
          "[dtls][teardown]")
{
    pdt::identity_fixture server_id("cn_srv");
    pdt::identity_fixture client_id("cn_cli");

    // The close_notify branch must post on_closed off its own drain_inbound stack:
    // the on_closed handler RESETS the owning unique_ptr (the natural consumer
    // reaction to a peer close), which would free `this` mid-drain_inbound if the
    // notify fired synchronously — a UAF the loop / try_complete() then touches.
    // Under asan this case is the proof. Looped to shake out any nondeterminism.
    constexpr int k_iterations = 50;
    int survived = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto h = std::make_unique<close_notify_harness>(server_id, client_id);
        h->complete_handshake();
        REQUIRE(h->server_complete);

        bool closed = false;
        // Destroy the server channel from INSIDE its own on_closed handler. If the
        // notify is synchronous (the pre-fix bug), reset() frees the channel while
        // drain_inbound is still on the stack -> UAF. The posted notify makes it safe.
        h->server_ch->on_closed([&] {
            closed = true;
            h->server_ch.reset();
        });

        h->client_send_close_notify();          // drives drain_inbound -> ZERO_RETURN
        auto bound = std::chrono::steady_clock::now() + std::chrono::milliseconds{1000};
        while(!closed && std::chrono::steady_clock::now() < bound)
        {
            h->io.poll();
            if(h->io.stopped())
                h->io.restart();
        }
        REQUIRE(closed);                         // the posted on_closed ran
        REQUIRE(h->server_ch == nullptr);        // and freed the channel cleanly
        ++survived;
    }
    REQUIRE(survived == k_iterations);
}

TEST_CASE("dtls.demux: a re-insert on an existing sender OVERWRITES the stale channel pointer",
          "[dtls][teardown]")
{
    // BL-01 secondary: insert must OVERWRITE (insert_or_assign), not no-op (emplace),
    // on an already-present key. A re-dial to a dest that still has a (stale) entry
    // otherwise silently keeps the OLD pointer and the new channel never routes.
    pdetail::basic_inbound_demux<ptls::dtls_channel> demux;

    ::asio::ip::udp::endpoint ep(::asio::ip::make_address("127.0.0.1"), 5555);

    // Two distinct non-null sentinels (never dereferenced — only their identity is
    // compared, so reinterpret_cast'd integers stand in for two channels).
    auto *first = reinterpret_cast<ptls::dtls_channel *>(0x1000);
    auto *second = reinterpret_cast<ptls::dtls_channel *>(0x2000);

    REQUIRE(demux.insert(ep, first));
    REQUIRE(demux.lookup(ep) == first);

    // Re-insert the SAME key with a new pointer: the lookup must now return the NEW
    // channel (the overwrite), not the stale first. emplace would keep `first`.
    REQUIRE(demux.insert(ep, second));
    REQUIRE(demux.lookup(ep) == second);
    REQUIRE(demux.size() == 1);                   // an overwrite does not grow the map
}

TEST_CASE("dtls.demux: the peer cap still bounds genuinely new senders after the insert change",
          "[dtls][teardown]")
{
    // The insert_or_assign change must not weaken the flood guard: NEW keys past the
    // cap are still refused, while an overwrite of a PRESENT key at the cap succeeds
    // (it does not grow the map).
    pdetail::basic_inbound_demux<ptls::dtls_channel> demux(/*max_peers=*/2);
    auto *p = reinterpret_cast<ptls::dtls_channel *>(0x1000);

    ::asio::ip::udp::endpoint a(::asio::ip::make_address("127.0.0.1"), 1);
    ::asio::ip::udp::endpoint b(::asio::ip::make_address("127.0.0.1"), 2);
    ::asio::ip::udp::endpoint c(::asio::ip::make_address("127.0.0.1"), 3);

    REQUIRE(demux.insert(a, p));
    REQUIRE(demux.insert(b, p));
    REQUIRE_FALSE(demux.insert(c, p));            // a NEW key past the cap is refused
    REQUIRE(demux.insert(a, p));                  // an overwrite of a PRESENT key still admits
    REQUIRE(demux.size() == 2);
}
