#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"

#include "plexus/asio/udp_server.h"

#include "plexus/io/io_error.h"
#include "plexus/io/fragmentation.h"

#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>

namespace pdt   = plexus::dtls_test;
namespace ptls  = plexus::tls;
namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

// A direct two-channel loopback (mirrors the channel_link in the handshake suite)
// that completes a live DTLS session, then drives the send/MTU path: each datagram a
// socket receives is fed to the OTHER channel's deliver_inbound. The configurable
// max_payload lets a section choose whether the configured cap or DTLS_get_data_mtu
// binds in the min(configured_cap, DTLS_get_data_mtu) reject.
struct mtu_link
{
    ::asio::io_context                  io;
    ptls::tls_credential                server_cred;
    ptls::tls_credential                client_cred;
    plexus::io::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    plexus::io::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server                   server_sock{io};
    pasio::udp_server                   client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool                                server_complete{false};
    bool                                client_complete{false};
    std::vector<std::vector<std::byte>> server_received;
    bool                                client_too_large{false};

    std::vector<std::vector<std::byte>> client_to_server;
    std::vector<std::vector<std::byte>> server_to_client;
    int client_records{0}; // client->server DTLS records since the last send

    mtu_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id,
             std::size_t max_payload,
             std::size_t record_mtu = ptls::dtls_channel::default_record_mtu)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"),
                                            server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"),
                                            client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(
                io, server_sock, client_ep, server_cred, server_cookie,
                ptls::dtls_channel::role::server, max_payload, record_mtu);
        client_ch = std::make_unique<ptls::dtls_channel>(
                io, client_sock, server_ep, client_cred, client_cookie,
                ptls::dtls_channel::role::client, max_payload, record_mtu);

        server_sock.on_datagram(
                [this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b)
                {
                    client_to_server.emplace_back(b.begin(), b.end());
                    ++client_records;
                });
        client_sock.on_datagram(
                [this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b)
                { server_to_client.emplace_back(b.begin(), b.end()); });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d)
                           { server_received.emplace_back(d.begin(), d.end()); });
        client_ch->on_error(
                [this](pio::io_error e)
                {
                    if(e == pio::io_error::message_too_large)
                        client_too_large = true;
                });
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

    // Send a frame of `size` bytes and pump until the server receives it OR an oversize
    // error fired. Returns true if the server received exactly one datagram of `size`.
    bool send_and_deliver(std::size_t               size,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds{2000})
    {
        server_received.clear();
        client_too_large = false;
        client_records   = 0;
        std::vector<std::byte> frame(size, std::byte{0xab});
        client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && !client_too_large &&
              std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
        return server_received.size() == 1 && server_received[0].size() == size;
    }

    // Send `size` and report whether it crossed as exactly ONE DTLS record (delivered
    // byte-equal). Above the encrypted record budget a frame still delivers, but fragmented
    // across MORE than one record — so one_record is the behavioral single-record boundary.
    bool delivered_in_one_record(std::size_t size)
    {
        return send_and_deliver(size) && client_records == 1;
    }

    // Probe the largest frame delivered in a SINGLE record by binary search over [lo, hi]:
    // single-record delivery is monotone in size (every size at or under the encrypted
    // budget rides one record, every larger size fragments into more than one), so the
    // boundary is discovered behaviorally — the test never reaches into the channel's
    // internals (no test-only DTLS_get_data_mtu accessor). `lo` must ride one record and
    // `hi` must fragment for the invariant to hold.
    std::size_t probe_one_record_ceiling(std::size_t lo, std::size_t hi)
    {
        if(!delivered_in_one_record(lo))
            return 0; // floor not single-record: window wrong
        std::size_t single = lo;
        std::size_t multi  = hi;
        while(multi - single > 1)
        {
            const std::size_t mid = single + (multi - single) / 2;
            if(delivered_in_one_record(mid))
                single = mid;
            else
                multi = mid;
        }
        return single;
    }
};

}

TEST_CASE("dtls.mtu: a frame at the data-MTU rides one record, one byte over fragments byte-equal, "
          "looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("mtu_srv");
    pdt::identity_fixture cli("mtu_cli");

    constexpr int k_iterations = 100;
    int           proven       = 0;
    std::size_t   observed_cap = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high configured cap so DTLS_get_data_mtu (the encrypted-record budget) is
        // the binding term of min(configured_cap, DTLS_get_data_mtu) (R-1).
        mtu_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // Find the exact accept/reject boundary. The single-record ceiling is the
        // encrypted record budget DTLS_get_data_mtu reports — the configured record MTU
        // (default_record_mtu) minus the DTLS 1.2 AEAD-GCM record overhead (13B header +
        // 8B explicit IV + 16B auth tag = 37B) minus the 3B udp envelope — so it lands
        // strictly below the configured budget. The bound is DERIVED from the record MTU
        // and a worst-case overhead ceiling, not a hardcoded number, so it tracks the
        // mtu_budget default rather than restating a stale 1400-era constant.
        constexpr std::size_t k_record_mtu          = ptls::dtls_channel::default_record_mtu;
        constexpr std::size_t k_max_record_overhead = 37u +
                plexus::wire::udp_envelope_overhead; // DTLS 1.2 AEAD-GCM record + udp envelope
        const std::size_t cap = l.probe_one_record_ceiling(100, k_record_mtu);
        REQUIRE(cap >= k_record_mtu -
                        k_max_record_overhead); // within the overhead band below the record MTU
        REQUIRE(cap < k_record_mtu);            // strictly below (overhead subtracted)
        observed_cap = cap;

        // The boundary frame is delivered intact AS ONE record (no fragmentation): the
        // server got exactly one on_data of exactly `cap` bytes over exactly one record.
        REQUIRE(l.delivered_in_one_record(cap));

        // One byte over the single-record ceiling no longer rejects — it FRAGMENTS across
        // more than one record and reassembles byte-equal into ONE on_data (the new
        // large-payload capability; the encrypted budget still governs each record).
        REQUIRE(l.send_and_deliver(cap + 1));
        REQUIRE(l.client_records > 1);
        REQUIRE_FALSE(l.client_too_large);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(observed_cap > 0);
}

TEST_CASE(
        "dtls.mtu: a low configured cap binds the single-record ceiling below the data-MTU, looped",
        "[dtls][mtu]")
{
    pdt::identity_fixture srv("cap_srv");
    pdt::identity_fixture cli("cap_cli");

    // A configured cap well BELOW DTLS_get_data_mtu so the configured term binds
    // min(configured_cap, DTLS_get_data_mtu) — proving the reject is the MIN of the two,
    // not DTLS_get_data_mtu alone.
    constexpr std::size_t k_cap = 200;

    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, k_cap);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // At the configured cap minus the envelope: delivered as one record (the configured
        // cap is the binding term, well under the encrypted data MTU).
        REQUIRE(l.delivered_in_one_record(k_cap - plexus::wire::udp_envelope_overhead));
        // Over the configured single-record ceiling: fragments (not rejects) and reassembles
        // byte-equal — the configured cap binds each fragment record, the message still flows.
        REQUIRE(l.send_and_deliver(k_cap + 1));
        REQUIRE(l.client_records > 1);
        REQUIRE_FALSE(l.client_too_large);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("dtls.mtu: a record MTU raised above the legacy 2048 drain buffer rides one record "
          "intact, looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("big_mtu_srv");
    pdt::identity_fixture cli("big_mtu_cli");

    // The drain buffer was a fixed 2048 bytes: SSL_read (message-oriented) would DISCARD a
    // record larger than the buffer and BIO_read would SPLIT one record across datagrams,
    // so a record_mtu above 2048 silently corrupted records. Sizing the buffer from the
    // configured record_mtu fixes both. Drive a record budget well above 2048 and assert a
    // frame near that ceiling crosses as exactly ONE record, delivered byte-equal.
    constexpr std::size_t k_record_mtu =
            4096; // above the legacy 2048 buffer, below the 8192 ceiling
    constexpr int k_iterations = 50;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, /*max_payload=*/100000, k_record_mtu);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // The single-record ceiling now tracks the raised record MTU (was pinned under 2048
        // by the old buffer). It lands within the AEAD-GCM + envelope overhead band below it.
        constexpr std::size_t k_max_record_overhead = 37u + plexus::wire::udp_envelope_overhead;
        const std::size_t     cap = l.probe_one_record_ceiling(2200, k_record_mtu);
        REQUIRE(cap > 2048); // exceeds the legacy fixed buffer
        REQUIRE(cap >= k_record_mtu -
                        k_max_record_overhead); // within the overhead band below the record MTU
        REQUIRE(cap < k_record_mtu);

        // The ceiling frame (> 2048 B) delivers intact as ONE record — pre-fix it was
        // truncated by SSL_read or split by BIO_read.
        REQUIRE(l.delivered_in_one_record(cap));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("dtls.mtu: a frame beyond the bounded max-message size is rejected via "
          "message_too_large, looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("big_srv");
    pdt::identity_fixture cli("big_cli");

    // The oversize reject is PRESERVED for a genuinely-too-big message: a frame beyond the
    // channel's per-MESSAGE size ceiling cannot fragment (it would exceed the receiver's hard
    // ceiling), so it is rejected at publish via on_error(message_too_large) and nothing
    // crosses the wire (the fail-closed bound on the fragment path). The bound is the
    // configurable node default (global_default_max_message_bytes) the channel is minted with,
    // not the old hardcoded fragmentation cap — a frame one byte past it is refused.
    constexpr int k_iterations = 30;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        REQUIRE_FALSE(l.send_and_deliver(pio::global_default_max_message_bytes + 1));
        REQUIRE(l.client_too_large);
        REQUIRE(l.server_received.empty());
        REQUIRE(l.client_records == 0); // nothing crossed the wire
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
