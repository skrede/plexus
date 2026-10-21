#ifndef HPP_GUARD_PLEXUS_TESTS_ASIO_UDP_LOOPBACK_H
#define HPP_GUARD_PLEXUS_TESTS_ASIO_UDP_LOOPBACK_H

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/endpoint.h"

#include "plexus/testing/pump.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
#include <utility>

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

}

#endif
