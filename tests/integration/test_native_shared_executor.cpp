#include "test_native_shared_executor_common.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/frame_router.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/wire/data_frame.h"

#include "plexus/log/logger.h"
#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/multicast_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <utility>
#include <optional>
#include <string_view>

using namespace native_shared_executor_fixture;

namespace {

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

std::string read_card_value(const std::vector<std::pair<std::string, std::string>> &card, std::string_view key)
{
    for(const auto &[k, v] : card)
        if(k == key)
            return v;
    return {};
}

}

// The generic user-owned-shared-executor property: a native multicast_discovery and an
// asio TCP transport progress to a result on ONE io_context the caller owns. The wired
// datagram seam carries the advertise -> announcement -> decode -> resolve cycle
// deterministically (no real multicast), while a framed payload round-trips over a real
// asio loopback channel — the SAME poll loop drives both subsystems' completions. This is
// the in-tree home of the shared-executor proof for the first-party discovery path (the
// mDNS-specific variant relocated to the opt-in interop example).
TEST_CASE("native discovery and plexus asio transport progress on one io_context", "[integration][asio]")
{
    ::asio::io_context io; // the SINGLE shared executor

    // --- Native discovery on the shared context ---
    wiring_datagram_socket sock_advertiser{io, "127.0.1.1"};
    wiring_datagram_socket sock_browser{io, "127.0.1.2"};
    sock_advertiser.pair_with(sock_browser);
    sock_browser.pair_with(sock_advertiser);

    plexus::discovery::multicast_discovery<wiring_datagram_socket, pasio::asio_policy> advertiser{io, sock_advertiser};
    plexus::discovery::multicast_discovery<wiring_datagram_socket, pasio::asio_policy> browser{io, sock_browser};

    int resolved_count = 0;
    std::vector<std::pair<std::string, std::string>> resolved_metadata;
    browser.browse(
            [&](const plexus::discovery::service_info &svc)
            {
                ++resolved_count;
                if(!svc.metadata.empty())
                    resolved_metadata = svc.metadata;
            });

    // The card is assembled the way a node assembles it (the codec demands a 32-hex
    // node_id and "plexus/<transport>/port" keys; a hand-rolled card would fail the
    // strict decode and emit no announcement).
    plexus::node_id advertised_id{};
    advertised_id[0]  = std::byte{0x11};
    advertised_id[15] = std::byte{0xb4};
    const auto advertised_hex = plexus::discovery::detail::hex_encode(advertised_id);
    plexus::discovery::service_info card{advertised_hex, {"tcp", "127.0.1.1:5555"},
                                         plexus::discovery::assemble_contact_card(advertised_id, {{"tcp", 5555}})};
    advertiser.advertise(card);

    // --- Transport round-trip on the SAME context ---
    pasio::asio_listener listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch) { server_channel = std::move(ch); });
    listener.start({"tcp", "127.0.0.1:0"});
    const auto port = listener.port();

    pasio::asio_channel client(io);
    std::optional<std::vector<std::byte>> received;
    plexus::log::null_logger router_sink;
    pio::frame_router router{router_sink};
    router.on_unidirectional(
            [&](const wire::frame_header &, std::span<const std::byte> inner)
            {
                auto decoded = wire::decode_unidirectional(inner);
                if(decoded)
                    received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
            });
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    while(!server_channel)
        io.poll_one();

    const auto payload    = bytes_of("shared-executor-payload");
    const std::string fqn = "demo._plexus._tcp.local.";
    plexus::log::null_logger sink;
    pio::message_forwarder<pasio::asio_policy> fwd{sink};
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "peer-node"};
    fwd.attach(sub, fqn);
    fwd.publish(fqn, payload);

    // Drive the ONE context: the SAME poll loop runs the TCP accept/read/write completions
    // AND the discovery announce/decode handlers posted on the wired seam. Both subsystems
    // progress to a result on the shared executor. Bounded so a regression fails fast.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(std::chrono::steady_clock::now() < deadline && (!received || resolved_count == 0))
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }

    browser.stop();
    advertiser.stop();
    io.poll();

    REQUIRE(received.has_value());
    REQUIRE(*received == payload); // the transport round-trip progressed on `io`
    // The advertised card resolved through the native discovery decode path on the SAME
    // io_context — positive evidence the discovery subsystem co-progressed on the shared
    // executor, not merely that its callback was armed.
    REQUIRE(resolved_count >= 1);
    REQUIRE(read_card_value(resolved_metadata, "node_id") == advertised_hex);
    REQUIRE(read_card_value(resolved_metadata, "plexus/tcp/port") == "5555");
}
