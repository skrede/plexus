#include "test_dtls_fragmentation_common.h"

using namespace dtls_fragmentation_fixture;

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
    static constexpr std::size_t k_socket_buf = 4u * 1024u * 1024u; // the host rmem_max/wmem_max ceiling

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

    large_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id, std::size_t max_message_bytes)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        const std::size_t budget = max_message_bytes + 16u * 1024u * 1024u;
        server_ch                = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred, server_cookie, ptls::dtls_channel::role::server, max_message_bytes,
                                                                        ptls::dtls_channel::default_record_mtu, max_message_bytes, budget, std::chrono::milliseconds{60000});
        client_ch                = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred, client_cookie, ptls::dtls_channel::role::client, max_message_bytes,
                                                                        ptls::dtls_channel::default_record_mtu, max_message_bytes, budget, std::chrono::milliseconds{60000});

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { client_to_server.emplace_back(b.begin(), b.end()); });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { server_to_client.emplace_back(b.begin(), b.end()); });

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

TEST_CASE("dtls.fragment: the lifted message ceiling fragments a message above the old 4 MiB cap "
          "(no reject), and a multi-MiB best-effort message reassembles byte-equal, looped",
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

    constexpr std::size_t k_ceiling  = 24u * 1024u * 1024u; // above 16 MB so a 16 MB send is admitted (not rejected)
    constexpr std::size_t k_reliable = 3u * 1024u * 1024u;  // the host-reliable best-effort ceiling (under rmem_max)

    constexpr int k_iterations = 3;
    int proven                 = 0;
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
        REQUIRE(l.client_error != pio::io_error::message_too_large); // fragmented, not rejected

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
