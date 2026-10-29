#ifndef HPP_GUARD_PLEXUS_TESTS_ASIO_UDP_LOOPBACK_H
#define HPP_GUARD_PLEXUS_TESTS_ASIO_UDP_LOOPBACK_H

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"

#include "plexus/datagram/detail/udp_reliable_arq.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"

#include "plexus/testing/pump.h"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::testing {

// A real loopback pair on one io_context: a listening server transport and a dialing
// client. The handshake ARQ establishes before any data flows; the harness captures
// the accepted server channel (the engine-owned channel the demux keeps routing to).
struct loopback
{
    ::asio::io_context io;
    plexus::asio::udp_transport server{io};
    plexus::asio::udp_transport client{io};

    std::unique_ptr<plexus::asio::udp_channel> accepted;
    std::unique_ptr<plexus::asio::udp_channel> dialed;

    loopback()
    {
        server.on_accepted([this](std::unique_ptr<plexus::asio::udp_channel> ch) { accepted = std::move(ch); });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until([this] { return server.port() != 0; });

        client.on_dialed([this](std::unique_ptr<plexus::asio::udp_channel> ch, const plexus::io::endpoint &) { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until([this] { return dialed != nullptr && accepted != nullptr; });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        plexus::testing::pump_until(io, pred, std::chrono::seconds(5));
    }

    void drain()
    {
        for(std::int32_t i = 0; i < 256; ++i)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

// A channel over its own server whose peer is a bound socket that never answers, so nothing ever acks it.
struct silent_peer
{
    ::asio::io_context io;
    plexus::asio::udp_server server;
    ::asio::ip::udp::socket peer;
    std::unique_ptr<plexus::asio::udp_channel> channel;

    silent_peer(plexus::datagram::detail::udp_channel_mode mode, plexus::datagram::detail::udp_arq_config arq, std::chrono::milliseconds reassembly_timeout)
            : io()
            , server(io)
            , peer(io, ::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), 0))
            , channel()
    {
        server.start(::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), 0));
        channel = std::make_unique<plexus::asio::udp_channel>(io, server, peer.local_endpoint(), plexus::asio::udp_channel::default_max_payload, arq, plexus::io::congestion::block,
                                                              plexus::asio::udp_channel::default_backpressure_bytes, mode, std::uint16_t{0},
                                                              plexus::io::global_default_max_message_bytes, plexus::io::reassembly_memory_budget, reassembly_timeout);
    }

    std::int32_t take_received()
    {
        std::int32_t taken = 0;
        std::array<std::byte, 2048> buf{};
        ::asio::ip::udp::endpoint from;
        std::error_code ec;
        while(peer.available(ec) != 0 && !ec)
        {
            peer.receive_from(::asio::buffer(buf), from, 0, ec);
            ++taken;
        }
        return taken;
    }
};

}

#endif
