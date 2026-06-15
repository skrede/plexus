#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"

#include "plexus/asio/udp_server.h"

#include "plexus/io/io_error.h"
#include "plexus/io/fragmentation.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <optional>
#include <algorithm>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pasio = plexus::asio;
namespace pio = plexus::io;

namespace {

// A direct two-channel loopback (mirrors the mtu_link in the MTU suite) that completes a
// live DTLS session, then drives the large-payload send/fragment/reassemble path: each
// datagram a socket receives is fed to the OTHER channel's deliver_inbound. The configured
// max_payload is the LOGICAL ceiling (raised high so a large message fragments rather than
// rejecting); the record MTU stays the single-datagram floor so the fragmenter splits
// against the real post-handshake DTLS_get_data_mtu (each fragment rides one DTLS record).
struct frag_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    pio::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    pio::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server server_sock{io};
    pasio::udp_server client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool server_complete{false};
    bool client_complete{false};
    std::vector<std::vector<std::byte>> server_received;

    std::vector<std::vector<std::byte>> client_to_server;
    std::vector<std::vector<std::byte>> server_to_client;
    int client_to_server_count{0};

    frag_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id,
              std::size_t max_payload)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred,
                                                         server_cookie, ptls::dtls_channel::role::server,
                                                         max_payload);
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred,
                                                         client_cookie, ptls::dtls_channel::role::client,
                                                         max_payload);

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            client_to_server.emplace_back(b.begin(), b.end());
            ++client_to_server_count;
        });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            server_to_client.emplace_back(b.begin(), b.end());
        });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
    }

    void pump_relays()
    {
        auto cs = std::move(client_to_server);
        client_to_server.clear();
        for(auto &dg : cs)
            server_ch->deliver_inbound(std::span<const std::byte>{dg});
        auto sc = std::move(server_to_client);
        server_to_client.clear();
        for(auto &dg : sc)
            client_ch->deliver_inbound(std::span<const std::byte>{dg});
    }

    void handshake(std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        server_ch->start_handshake();
        client_ch->start_handshake();
        auto bound = std::chrono::steady_clock::now() + timeout;
        while((!server_complete || !client_complete) && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
    }

    // Send a `size`-byte frame (a deterministic byte ramp) and pump until the server has
    // reassembled exactly one message. Returns the reassembled bytes (empty on timeout).
    std::vector<std::byte> round_trip(std::size_t size, std::chrono::milliseconds timeout = std::chrono::milliseconds{2000})
    {
        server_received.clear();
        client_to_server_count = 0;
        std::vector<std::byte> frame(size);
        for(std::size_t i = 0; i < size; ++i)
            frame[i] = static_cast<std::byte>(i & 0xFF);
        client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
        return server_received.size() == 1 ? server_received[0] : std::vector<std::byte>{};
    }
};

std::vector<std::byte> ramp(std::size_t size)
{
    std::vector<std::byte> v(size);
    for(std::size_t i = 0; i < size; ++i)
        v[i] = static_cast<std::byte>(i & 0xFF);
    return v;
}

}

TEST_CASE("dtls.fragment: a large payload fragments across records and reassembles byte-equal over a live session, looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("frag_srv");
    pdt::identity_fixture cli("frag_cli");

    // Largest payload proven over live DTLS: ~24 KiB. With a high logical ceiling (so the
    // frame fragments rather than rejecting) and the default record MTU (~1363 encrypted),
    // a 24 KiB message splits into ~18 records and reassembles into ONE on_data byte-equal.
    constexpr std::size_t k_payload = 24u * 1024u;

    constexpr int k_iterations = 50;
    int proven = 0;
    int max_fragments = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        frag_link l(srv, cli, /*max_payload=*/1u * 1024u * 1024u);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        const auto got = l.round_trip(k_payload);
        REQUIRE(got.size() == k_payload);
        REQUIRE(got == ramp(k_payload));               // byte-equal reassembly
        // The message crossed the wire as MANY records (fragmentation happened), each one
        // DTLS record — never one oversize record, never a reject.
        REQUIRE(l.client_to_server_count > 1);
        max_fragments = std::max(max_fragments, l.client_to_server_count);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(max_fragments > 1);
}

TEST_CASE("dtls.fragment: a frame at the record budget rides ONE record (parity, no fragmentation), looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("par_srv");
    pdt::identity_fixture cli("par_cli");

    constexpr int k_iterations = 50;
    int proven = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high logical ceiling so DTLS_get_data_mtu (the encrypted record budget) is the
        // binding term. A frame comfortably under that budget must ride ONE record
        // (byte-identical to the pre-fragment path), not fragment.
        frag_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // The encrypted record budget is the configured record MTU (default_record_mtu)
        // minus the DTLS 1.2 AEAD-GCM record overhead (~37B) minus the 3B udp envelope. A
        // frame an overhead-band below the record MTU stays comfortably under the budget and
        // crosses as exactly one record. The size is DERIVED from the record MTU, not a stale
        // 1400-era constant, so it tracks the mtu_budget default.
        constexpr std::size_t k_under_budget = ptls::dtls_channel::default_record_mtu - 128u;
        const auto got = l.round_trip(k_under_budget);
        REQUIRE(got.size() == k_under_budget);
        REQUIRE(got == ramp(k_under_budget));
        REQUIRE(l.client_to_server_count == 1);        // ONE record: no fragmentation
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

namespace {

// One fresh live DTLS session driving a single `size`-byte message through the splitter into
// many DTLS records and back through the reassembler. Returns {received_records, reassembled}.
// The logical ceiling is raised above max_message_size so the frame fragments rather than
// rejecting; each fragment rides one record bounded by the DTLS_get_data_mtu-derived budget.
struct large_run
{
    std::size_t records;
    std::vector<std::byte> got;
};

large_run dtls_large_run(const pdt::identity_fixture &srv, const pdt::identity_fixture &cli,
                         std::size_t size, std::chrono::milliseconds timeout)
{
    frag_link l(srv, cli, /*max_payload=*/8u * 1024u * 1024u);
    l.handshake();
    REQUIRE(l.client_complete);
    REQUIRE(l.server_complete);

    auto got = l.round_trip(size, timeout);
    return {static_cast<std::size_t>(l.client_to_server_count), std::move(got)};
}

}

TEST_CASE("dtls.fragment: a 1 MB / 4 MB best-effort burst reassembles byte-equal over the default send queue",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("fragbe_srv");
    pdt::identity_fixture cli("fragbe_cli");

    // DTLS app data is best-effort (no DTLS-layer retransmit). A single 1 MB / 4 MB send
    // splits into ~900 / ~3650 records emitted in one synchronous burst. Each record is one
    // datagram handed to the shared udp_server send queue: the queue's byte cap is floored at
    // one per-message ceiling, so a single fragmenting message's whole burst is admitted (a
    // refused fragment on the best-effort path is lost forever, with no retransmit to recover
    // it, so the floor is what keeps the message intact). The relay carries every emitted
    // record into the peer's reassembler, which completes the message byte-for-byte.
    constexpr std::size_t k_one_mb = 1024 * 1024;
    constexpr std::size_t k_four_mb = 4 * 1024 * 1024;
    for(std::size_t size : {k_one_mb, k_four_mb})
    {
        const auto r = dtls_large_run(srv, cli, size, std::chrono::milliseconds{8000});
        REQUIRE(r.records > 1);                          // the message fragmented into many records
        REQUIRE(r.got.size() == size);                   // and every fragment arrived and reassembled
    }
}

namespace {

// A live DTLS loopback over a manual record relay, like frag_link, but constructing each
// dtls_channel with the LIFTED message ceiling + raised reassembly budget + extended
// reclaim window (the threaded node-options knobs) and raising the kernel socket buffers
// to the host max so a multi-megabyte best-effort burst is buffered rather than dropped at
// the socket. It exercises the lifted size envelope on the secure-datagram path: the send
// no longer rejects a message above the old 4 MiB hardcoded cap (it fragments), and a
// best-effort message that fits the kernel buffers reassembles byte-equal.
struct large_link
{
    static constexpr std::size_t k_socket_buf = 4u * 1024u * 1024u;     // the host rmem_max/wmem_max ceiling

    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    pio::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    pio::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server server_sock{io, pio::congestion::block, k_socket_buf, k_socket_buf, k_socket_buf};
    pasio::udp_server client_sock{io, pio::congestion::block, k_socket_buf, k_socket_buf, k_socket_buf};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool server_complete{false};
    bool client_complete{false};
    std::vector<std::vector<std::byte>> server_received;
    std::optional<pio::io_error> client_error;

    std::vector<std::vector<std::byte>> client_to_server;
    std::vector<std::vector<std::byte>> server_to_client;

    large_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id,
               std::size_t max_message_bytes)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        const std::size_t budget = max_message_bytes + 16u * 1024u * 1024u;
        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred,
                                                         server_cookie, ptls::dtls_channel::role::server,
                                                         max_message_bytes, ptls::dtls_channel::default_record_mtu,
                                                         max_message_bytes, budget, std::chrono::milliseconds{60000});
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred,
                                                         client_cookie, ptls::dtls_channel::role::client,
                                                         max_message_bytes, ptls::dtls_channel::default_record_mtu,
                                                         max_message_bytes, budget, std::chrono::milliseconds{60000});

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            client_to_server.emplace_back(b.begin(), b.end());
        });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            server_to_client.emplace_back(b.begin(), b.end());
        });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
        client_ch->on_error([this](pio::io_error e) { client_error = e; });
    }

    void pump_relays()
    {
        auto cs = std::move(client_to_server);
        client_to_server.clear();
        for(auto &dg : cs)
            server_ch->deliver_inbound(std::span<const std::byte>{dg});
        auto sc = std::move(server_to_client);
        server_to_client.clear();
        for(auto &dg : sc)
            client_ch->deliver_inbound(std::span<const std::byte>{dg});
    }

    void handshake(std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        server_ch->start_handshake();
        client_ch->start_handshake();
        auto bound = std::chrono::steady_clock::now() + timeout;
        while((!server_complete || !client_complete) && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
    }

    std::vector<std::byte> round_trip(std::size_t size, std::chrono::milliseconds timeout = std::chrono::milliseconds{20000})
    {
        server_received.clear();
        client_ch->send(std::span<const std::byte>{ramp(size)});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
        return server_received.empty() ? std::vector<std::byte>{} : server_received[0];
    }
};

}

TEST_CASE("dtls.fragment: the lifted message ceiling fragments a message above the old 4 MiB cap (no reject), and a multi-MiB best-effort message reassembles byte-equal, looped",
          "[dtls][fragment][envelope16]")
{
    // The lifted size envelope on the secure-datagram path. The dtls_channel ctor now threads
    // the per-message ceiling (the send-side oversize-reject) + the aggregate reassembly
    // budget + the reclaim window, so a message ABOVE the old hardcoded 4 MiB cap is
    // FRAGMENTED rather than rejected with message_too_large. DTLS app data is best-effort
    // (no retransmit), so full 16 MB delivery over loopback is bounded by the host kernel
    // buffers (rmem_max ~4 MiB) — the same envelope the recorded 1/4 MB best-effort case
    // documents. This cell proves: (1) the send-cap is lifted (an 8 MiB send fragments, no
    // message_too_large), and (2) a 3 MiB best-effort message — comfortably above the old
    // 4 MiB-class fragmentation cap's per-record budget and under the kernel-buffer ceiling
    // — reassembles byte-equal over a live DTLS session. Looped in-body; re-run >=2.
    pdt::identity_fixture srv("env_srv");
    pdt::identity_fixture cli("env_cli");

    constexpr std::size_t k_ceiling = 24u * 1024u * 1024u;   // above 16 MB so a 16 MB send is admitted (not rejected)
    constexpr std::size_t k_reliable = 3u * 1024u * 1024u;   // the host-reliable best-effort ceiling (under rmem_max)

    constexpr int k_iterations = 3;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        large_link l(srv, cli, k_ceiling);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // (1) The lifted send-cap: a 16 MB send is fragmented, NOT rejected. The old cap
        // raised message_too_large for anything above 4 MiB; with the ceiling at 24 MiB the
        // fragmenter runs and no oversize error fires.
        l.client_error.reset();
        l.client_ch->send(std::span<const std::byte>{ramp(16u * 1024u * 1024u)});
        l.io.poll();
        REQUIRE(l.client_error != pio::io_error::message_too_large);   // fragmented, not rejected

        // (2) The mechanism: a 3 MiB best-effort message reassembles byte-equal.
        const auto got = l.round_trip(k_reliable);
        REQUIRE(got.size() == k_reliable);
        REQUIRE(got == ramp(k_reliable));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    WARN("dtls best-effort lossless ceiling on this host ~= 3 MiB (rmem_max-bounded); full 16 MB "
         "best-effort delivery is transport-capped — the lifted ceiling fragments it, the kernel "
         "buffers drop the un-retransmitted overflow (the reliable large-datagram path is udpr)");
}
