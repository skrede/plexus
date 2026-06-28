// Real-mDNS interop demo: advertise a plexus contact card over real mDNS, browse
// for it, and round-trip a payload over an asio TCP channel — all driven by ONE
// io_context the caller owns. It is the living proof that the public
// plexus::discovery contract suffices for a real third-party backend (mdnspp), and
// that a discovery resolve and a transport round-trip co-progress on one executor.

#include "plexus/mdnspp/mdnspp_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/frame_router.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/wire/data_frame.h"

#include "plexus/log/logger.h"
#include "plexus/discovery/discovery.h"

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

namespace pasio = plexus::asio;
namespace pmdns = plexus::mdnspp;
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

int main()
{
    ::asio::io_context io; // the SINGLE shared executor

    // Advertise a local service and browse for it on the SAME io_context, so a real
    // mDNS resolve (announce -> query -> aggregate) progresses on the shared executor.
    pmdns::mdnspp_discovery advertiser(io, "_plexus._tcp.local.");
    plexus::discovery::service_info local{"probe._plexus._tcp.local.", {"tcp", "127.0.0.1:5555"}, {{"node_id", "0011"}, {"plexus/tcp/port", "5555"}}};
    advertiser.advertise(local);

    pmdns::mdnspp_discovery discovery(io, "_plexus._tcp.local.");
    int resolved_count = 0;
    std::vector<std::pair<std::string, std::string>> resolved_metadata;
    discovery.browse(
            [&](const plexus::discovery::service_info &svc)
            {
                ++resolved_count;
                if(!svc.metadata.empty())
                    resolved_metadata = svc.metadata;
            });

    // A transport round-trip on the SAME context: an asio TCP listener + client
    // exchange a framed payload while the mDNS browse converges.
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

    // Drive the ONE context: the SAME poll loop runs the TCP completions AND the mDNS
    // announce/query/recv/timer handlers. Run past the round-trip AND the browse's
    // silence timeout so both subsystems progress to a result on the shared context.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while(std::chrono::steady_clock::now() < deadline && (!received || resolved_count == 0))
        io.poll();

    discovery.stop();
    advertiser.stop();
    io.poll();

    const bool round_tripped = received.has_value() && *received == payload;
    std::cout << "transport round-trip: " << (round_tripped ? "ok" : "incomplete") << '\n';
    std::cout << "mdns resolves: " << resolved_count << '\n';
    if(resolved_count > 0)
    {
        std::cout << "resolved node_id: " << read_card_value(resolved_metadata, "node_id") << '\n';
        std::cout << "resolved plexus/tcp/port: " << read_card_value(resolved_metadata, "plexus/tcp/port") << '\n';
    }

    return (round_tripped && resolved_count >= 1) ? 0 : 1;
}
