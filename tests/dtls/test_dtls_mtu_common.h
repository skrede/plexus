#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_MTU_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_MTU_COMMON_H

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

namespace dtls_mtu_fixture {

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
    int                                 client_records{0}; // client->server DTLS records since the last send

    mtu_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id, std::size_t max_payload, std::size_t record_mtu = ptls::dtls_channel::default_record_mtu)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred, server_cookie, ptls::dtls_channel::role::server, max_payload, record_mtu);
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred, client_cookie, ptls::dtls_channel::role::client, max_payload, record_mtu);

        server_sock.on_datagram(
                [this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b)
                {
                    client_to_server.emplace_back(b.begin(), b.end());
                    ++client_records;
                });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { server_to_client.emplace_back(b.begin(), b.end()); });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
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
    bool send_and_deliver(std::size_t size, std::chrono::milliseconds timeout = std::chrono::milliseconds{2000})
    {
        server_received.clear();
        client_too_large = false;
        client_records   = 0;
        std::vector<std::byte> frame(size, std::byte{0xab});
        client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && !client_too_large && std::chrono::steady_clock::now() < bound)
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

#endif
