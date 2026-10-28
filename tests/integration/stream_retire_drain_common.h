#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_STREAM_RETIRE_DRAIN_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_STREAM_RETIRE_DRAIN_COMMON_H

#include "floored_tcp.h"

#include "plexus/asio/asio_channel.h"
#if !defined(_WIN32)
    #include "plexus/asio/unix_channel.h"
#endif

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>
#if !defined(_WIN32)
    #include <asio/local/connect_pair.hpp>
#endif

#include <vector>
#include <cstddef>

namespace stream_retire_drain {

// The peer never reads and both kernel buffers are floored, so a write of a few hundred KiB stays pending.
struct tcp_pair
{
    using channel = plexus::asio::asio_channel;

    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc;
    ::asio::ip::tcp::socket peer;
    ::asio::ip::tcp::socket client;

    tcp_pair()
            : io()
            , acc(floored_tcp::loopback_acceptor(io))
            , peer(io)
            , client(io)
    {
        floored_tcp::connect(client, acc.local_endpoint());
        acc.accept(peer);
    }
};

#if !defined(_WIN32)
struct unix_pair
{
    using channel = plexus::asio::unix_channel;

    ::asio::io_context io;
    ::asio::local::stream_protocol::socket peer;
    ::asio::local::stream_protocol::socket client;

    unix_pair()
            : io()
            , peer(io)
            , client(io)
    {
        ::asio::local::connect_pair(peer, client);
    }
};
#endif

// Every byte of one frame but its last eight, so the reader holds a frame in progress and arms its no-progress wait.
inline std::vector<std::byte> partial_frame()
{
    plexus::wire::frame_header hdr{};
    hdr.type = plexus::wire::msg_type::unidirectional;
    const std::vector<std::byte> body(64, std::byte{0x42});
    auto out = plexus::wire::encode_frame(hdr, body);
    out.resize(out.size() - 8);
    return out;
}

}

#endif
