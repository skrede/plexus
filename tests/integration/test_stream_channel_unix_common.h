#ifndef HPP_GUARD_TESTS_INTEGRATION_STREAM_CHANNEL_UNIX_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_STREAM_CHANNEL_UNIX_COMMON_H

// Gated real-AF_UNIX stream-channel hardening leg over a local socket. A raw client
// socket writes hostile bytes into a live unix_channel (the accepted server end,
// stamped with a SHORT-floor stream::stream_inbound_config) and the channel's
// on_protocol_close fires with the matching close_cause — the byte-stream framing
// defense proven end-to-end over a real local socket, not a virtual-clock oracle:
//   * garbage  : a bad-magic byte run -> close_cause::invalid_magic.
//   * slowloris: a valid frame header claiming N payload bytes, the payload
//                withheld -> close_cause::no_progress_timeout after the frame's
//                size-proportional deadline.
// on_protocol_close is the seam DISTINCT from on_error: this leg asserts the
// CHANNEL callback fires (the session-level no-re-dial discrimination is proven by
// its own oracle). Each behavioral path loops in-body; the ctest invocation is
// re-run >=3 process runs for cross-process reproducibility (a live-networking
// claim is never made from one run). This is the dialer->acceptor frame-desync
// defense at the channel layer for the local-stream transport.

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/unix_policy.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/stream/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/write.hpp>
#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <unistd.h>

#include <span>
#include <array>
#include <vector>
#include <string>
#include <chrono>
#include <memory>
#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <algorithm>

namespace pasio  = plexus::asio;
namespace wire   = plexus::wire;
namespace stream = plexus::stream;

namespace stream_channel_unix_fixture {

// A short-floor config so the slowloris leg trips fast: a few hundred ms floor and
// a tiny throughput so even a small withheld payload's size-proportional deadline
// stays short. Passed EXPLICITLY — the 30s default would make the leg glacial.
inline stream::stream_inbound_config short_cfg()
{
    return stream::stream_inbound_config{.no_progress_floor = std::chrono::milliseconds(200), .min_throughput_bytes_per_sec = 64};
}

// A per-instance owner-only temp directory + a SHORT socket path within it.
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char tmpl[]      = "/tmp/pxu-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir              = made ? made : "";
        path             = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// Stand up a real local-socket pair: a unix_listener accepts a short-config server
// channel, a raw client socket connects to the bound path. The harness owns both
// ends and pumps one io_context.
struct loopback
{
    temp_sock sock;
    ::asio::io_context io;
    pasio::unix_listener listener{io, short_cfg()};
    std::unique_ptr<pasio::unix_channel> server;
    ::asio::local::stream_protocol::socket client{io};

    std::optional<wire::close_cause> caused;
    int closes{0};

    loopback()
    {
        listener.on_accepted(
                [this](std::unique_ptr<pasio::unix_channel> ch)
                {
                    server = std::move(ch);
                    server->on_protocol_close(
                            [this](wire::close_cause c)
                            {
                                caused = c;
                                ++closes;
                            });
                });
        listener.start({"unix", sock.path});

        client.connect(::asio::local::stream_protocol::endpoint(sock.path));
        pump_until([this] { return server != nullptr; });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void write_raw(std::span<const std::byte> bytes)
    {
        ::asio::write(client, ::asio::buffer(bytes.data(), bytes.size()));
    }
};

// A valid frame header that claims payload_len bytes, with NO payload following —
// the slowloris shape: the reassembler buffers the header + waits for a payload
// that never comes, so the no-progress deadline fires.
inline std::array<std::byte, wire::header_size> withholding_header(std::uint64_t payload_len)
{
    wire::frame_header hdr{};
    hdr.type         = wire::msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = 0;
    hdr.timestamp_ns = 0;
    hdr.payload_len  = payload_len;
    return wire::encode_header(hdr);
}

}

#endif
