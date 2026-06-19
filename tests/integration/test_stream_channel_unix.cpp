// Gated real-AF_UNIX stream-channel hardening leg over a local socket. A raw client
// socket writes hostile bytes into a live unix_channel (the accepted server end,
// stamped with a SHORT-floor wire::stream_inbound_config) and the channel's
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
#include "plexus/wire/stream_inbound.h"

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

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;

namespace {

// A short-floor config so the slowloris leg trips fast: a few hundred ms floor and
// a tiny throughput so even a small withheld payload's size-proportional deadline
// stays short. Passed EXPLICITLY — the 30s default would make the leg glacial.
wire::stream_inbound_config short_cfg()
{
    return wire::stream_inbound_config{.no_progress_floor = std::chrono::milliseconds(200),
                                       .min_throughput_bytes_per_sec = 64};
}

// A per-instance owner-only temp directory + a SHORT socket path within it.
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char        tmpl[] = "/tmp/pxu-XXXXXX";
        const char *made   = ::mkdtemp(tmpl);
        dir                = made ? made : "";
        path               = dir + "/s";
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
    temp_sock                              sock;
    ::asio::io_context                     io;
    pasio::unix_listener                   listener{io, short_cfg()};
    std::unique_ptr<pasio::unix_channel>   server;
    ::asio::local::stream_protocol::socket client{io};

    std::optional<wire::close_cause> caused;
    int                              closes{0};

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
std::array<std::byte, wire::header_size> withholding_header(std::uint64_t payload_len)
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

TEST_CASE("unix stream channel: a bad-magic byte run fires on_protocol_close(invalid_magic)",
          "[integration][unix][hardening]")
{
    constexpr int k_iterations = 20;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        loopback h;
        REQUIRE(h.server != nullptr);

        // A full header-size run of non-magic bytes: the reassembler only validates
        // the magic once it has a whole header (>= header_size bytes), so a short
        // run would merely buffer as a partial header. A header-size+ garbage run
        // makes decode_header reject the bad magic and stream_inbound raise
        // invalid_magic.
        std::array<std::byte, wire::header_size + 4> garbage{};
        garbage.fill(std::byte{0xFF});
        h.write_raw(garbage);

        h.pump_until([&] { return h.closes >= 1; });
        REQUIRE(h.closes == 1);
        REQUIRE(h.caused.has_value());
        REQUIRE(*h.caused == wire::close_cause::invalid_magic);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("unix stream channel: a header with a withheld payload fires "
          "on_protocol_close(no_progress_timeout)",
          "[integration][unix][hardening]")
{
    constexpr int k_iterations = 20;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        loopback h;
        REQUIRE(h.server != nullptr);

        // A valid header claiming a small payload, then nothing: the reassembler
        // holds an in-progress frame and the size-proportional deadline elapses. The
        // payload is kept small (8 bytes) so the 200ms floor dominates the deadline
        // (8 / 64 B/s = 125ms < floor) and each iteration trips fast.
        auto header = withholding_header(/*payload_len=*/8);
        h.write_raw(header);

        h.pump_until([&] { return h.closes >= 1; });
        REQUIRE(h.closes == 1);
        REQUIRE(h.caused.has_value());
        REQUIRE(*h.caused == wire::close_cause::no_progress_timeout);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("stream_channel_unix bound: congestion verdicts are byte-identical to asio_channel",
          "[integration][unix][bound]")
{
    // unix_channel composes the shared bounded stream_send_queue, so its congestion verdicts
    // are byte-identical to asio_channel's — proven over a real (connected) local socket
    // whose PEER NEVER READS, so the kernel send buffer fills and async_write stalls; the
    // userspace outbox then accumulates and the byte cap engages. A small cap (4 KiB) bounds
    // it; 1 KiB frames fill it, after which drop_newest sheds (dropped_count advances) and
    // block surfaces would_block — the SAME edge asio_channel proves, and the outbox NEVER
    // grows past the cap.
    namespace pio = plexus::io;
    auto run      = [](pio::congestion mode)
    {
        constexpr std::size_t                    cap = 4096;
        temp_sock                                sock;
        ::asio::io_context                       io;
        ::asio::local::stream_protocol::acceptor acc{
                io, ::asio::local::stream_protocol::endpoint(sock.path)};
        ::asio::local::stream_protocol::socket peer{io};
        ::asio::local::stream_protocol::socket client{io};
        client.connect(::asio::local::stream_protocol::endpoint(sock.path));
        acc.accept(peer); // peer adopts but NEVER reads

        pasio::unix_channel ch{io, std::move(client), wire::stream_inbound_config{}, mode,
                               pio::egress_capacity::of_bytes(cap)};
        REQUIRE(ch.congestion_mode() == mode);

        std::optional<pio::io_error> err;
        ch.on_error([&](pio::io_error e) { err = e; });

        std::vector<std::byte> kib(1024, std::byte{0x5A});
        for(int i = 0; i < 4096; ++i)
        {
            ch.send(kib);
            io.poll();
            REQUIRE(ch.backpressured() <= cap); // NEVER grows past the cap
            if(mode == pio::congestion::drop_newest && ch.dropped_count() > 0)
                break;
            if(mode == pio::congestion::block && err.has_value())
                break;
        }

        if(mode == pio::congestion::drop_newest)
            REQUIRE(ch.dropped_count() > 0); // shed at the publisher (the asio verdict)
        else
        {
            REQUIRE(err.has_value());
            REQUIRE(*err == pio::io_error::would_block); // block stalls (the asio verdict)
            REQUIRE(ch.dropped_count() == 0);
        }
        REQUIRE(ch.backpressured() <= cap);
    };

    run(plexus::io::congestion::drop_newest);
    run(plexus::io::congestion::block);
}

namespace {

std::vector<std::byte> ramp_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

}

TEST_CASE("unix stream channel: a 16 MB single frame round-trips byte-identically over a real "
          "local-socket pair, looped",
          "[integration][unix][envelope16]")
{
    // The lifted message-size envelope on the AF_UNIX stream path: one 16 MiB frame over a
    // real connected local-socket pair, reassembled byte-equal. The receive channel raises
    // its reassembler ceiling + buffer cap above the payload; the send channel's write-queue
    // byte cap holds the whole framed message under congestion=block. Looped in-body and
    // re-run across process runs (a transport claim is never made from one run); a position
    // ramp in the body catches a reorder/corruption, not just a size mismatch.
    namespace pio                         = plexus::io;
    constexpr std::size_t       k_payload = 16u * 1024u * 1024u;
    constexpr std::size_t       k_ceiling = 20u * 1024u * 1024u;
    wire::stream_inbound_config cfg{};
    cfg.max_payload_size          = k_ceiling;
    cfg.buffered_bytes_cap        = k_ceiling + wire::header_size;
    constexpr std::size_t k_queue = k_ceiling + 4u * 1024u * 1024u;

    constexpr int k_iterations = 3;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        temp_sock                                sock;
        ::asio::io_context                       io;
        ::asio::local::stream_protocol::acceptor acc{
                io, ::asio::local::stream_protocol::endpoint(sock.path)};
        ::asio::local::stream_protocol::socket raw_server{io};
        ::asio::local::stream_protocol::socket raw_client{io};
        raw_client.connect(::asio::local::stream_protocol::endpoint(sock.path));
        acc.accept(raw_server);

        pasio::unix_channel server{io, std::move(raw_server), cfg, pio::congestion::block,
                                   pio::egress_capacity::of_bytes(k_queue)};
        pasio::unix_channel client{io, std::move(raw_client), cfg, pio::congestion::block,
                                   pio::egress_capacity::of_bytes(k_queue)};

        std::vector<std::byte>           got;
        std::optional<wire::close_cause> closed;
        server.on_data([&](std::span<const std::byte> d) { got.assign(d.begin(), d.end()); });
        server.on_protocol_close([&](wire::close_cause c) { closed = c; });

        const auto         body = ramp_payload(k_payload);
        wire::frame_header hdr{};
        hdr.type         = wire::msg_type::unidirectional;
        hdr.payload_len  = body.size();
        const auto frame = wire::encode_frame(hdr, std::span<const std::byte>{body});
        client.send(std::span<const std::byte>{frame});

        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while(got.size() < frame.size() && !closed && std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE_FALSE(closed.has_value()); // the receive ceiling admitted the 16 MB frame
        REQUIRE(got.size() == frame.size());
        REQUIRE(got == frame); // byte-equal reassembly, no reorder/corruption
        const auto delivered_body = std::span<const std::byte>{got}.subspan(wire::header_size);
        REQUIRE(std::equal(delivered_body.begin(), delivered_body.end(), body.begin()));

        client.close();
        server.close();
        io.poll();
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
