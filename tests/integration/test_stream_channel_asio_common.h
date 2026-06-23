#ifndef HPP_GUARD_TESTS_INTEGRATION_STREAM_CHANNEL_ASIO_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_STREAM_CHANNEL_ASIO_COMMON_H

// Gated real-TCP stream-channel hardening leg over asio loopback. A raw client
// socket writes hostile bytes into a live asio_channel (the accepted server end,
// stamped with a SHORT-floor stream::stream_inbound_config) and the channel's
// on_protocol_close fires with the matching close_cause — the byte-stream framing
// defense proven end-to-end over a real socket, not a virtual-clock oracle:
//   * garbage  : a bad-magic byte run -> close_cause::invalid_magic.
//   * slowloris: a valid frame header claiming N payload bytes, the payload
//                withheld -> close_cause::no_progress_timeout after the frame's
//                size-proportional deadline.
// on_protocol_close is the seam DISTINCT from on_error: this leg asserts the
// CHANNEL callback fires (the session-level no-re-dial discrimination is proven by
// its own oracle). Each behavioral path loops in-body; the ctest invocation is
// re-run >=3 process runs for cross-process reproducibility (a live-networking
// claim is never made from one run).

#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/asio_policy.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/stream/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/write.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <algorithm>
#include <memory>
#include <vector>
#include <cstring>
#include <cstdint>
#include <cstddef>
#include <optional>

namespace pasio  = plexus::asio;
namespace pio    = plexus::io;
namespace wire   = plexus::wire;
namespace stream = plexus::stream;

namespace stream_channel_asio_fixture {

// A short-floor config so the slowloris leg trips fast: a few hundred ms floor and
// a tiny throughput so even a small withheld payload's size-proportional deadline
// stays short. Passed EXPLICITLY — the 30s default would make the leg glacial.
inline stream::stream_inbound_config short_cfg()
{
    return stream::stream_inbound_config{.no_progress_floor = std::chrono::milliseconds(200),
                                         .min_throughput_bytes_per_sec = 64};
}

// Stand up a real loopback pair: an asio_listener accepts a short-config server
// channel, a raw client socket connects to it. The harness owns both ends and
// pumps one io_context.
struct loopback
{
    ::asio::io_context                   io;
    pasio::asio_listener                 listener{io, short_cfg()};
    std::unique_ptr<pasio::asio_channel> server;
    ::asio::ip::tcp::socket              client{io};

    std::optional<wire::close_cause> caused;
    int                              closes{0};

    loopback()
    {
        listener.on_accepted(
                [this](std::unique_ptr<pasio::asio_channel> ch)
                {
                    server = std::move(ch);
                    server->on_protocol_close(
                            [this](wire::close_cause c)
                            {
                                caused = c;
                                ++closes;
                            });
                });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.connect(ep);
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

// The payload body of a delivered complete frame (header || body): skip the fixed
// header and read the remaining bytes as a string. Used by the throughput proof to
// assert each gathered frame arrived intact and in order.
inline std::string data_body(const std::vector<std::byte> &frame)
{
    if(frame.size() < wire::header_size)
        return {};
    auto body = std::span<const std::byte>{frame}.subspan(wire::header_size);
    return std::string{reinterpret_cast<const char *>(body.data()), body.size()};
}

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

// A deterministic position-dependent payload, regenerated to byte-check the round-trip.
inline std::vector<std::byte> ramp_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

}

#endif
