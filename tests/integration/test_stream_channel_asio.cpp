// Gated real-TCP stream-channel hardening leg over asio loopback. A raw client
// socket writes hostile bytes into a live asio_channel (the accepted server end,
// stamped with a SHORT-floor wire::stream_inbound_config) and the channel's
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
#include "plexus/wire/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/write.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <vector>
#include <cstring>
#include <cstdint>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace pio = plexus::io;
namespace wire = plexus::wire;

namespace {

// A short-floor config so the slowloris leg trips fast: a few hundred ms floor and
// a tiny throughput so even a small withheld payload's size-proportional deadline
// stays short. Passed EXPLICITLY — the 30s default would make the leg glacial.
wire::stream_inbound_config short_cfg()
{
    return wire::stream_inbound_config{
        .no_progress_floor = std::chrono::milliseconds(200),
        .min_throughput_bytes_per_sec = 64};
}

// Stand up a real loopback pair: an asio_listener accepts a short-config server
// channel, a raw client socket connects to it. The harness owns both ends and
// pumps one io_context.
struct loopback
{
    ::asio::io_context io;
    pasio::asio_listener listener{io, short_cfg()};
    std::unique_ptr<pasio::asio_channel> server;
    ::asio::ip::tcp::socket client{io};

    std::optional<wire::close_cause> caused;
    int closes{0};

    loopback()
    {
        listener.on_accepted([this](std::unique_ptr<pasio::asio_channel> ch) {
            server = std::move(ch);
            server->on_protocol_close([this](wire::close_cause c) { caused = c; ++closes; });
        });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.connect(ep);
        pump_until([this] { return server != nullptr; });
    }

    template <typename Pred>
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
std::string data_body(const std::vector<std::byte> &frame)
{
    if(frame.size() < wire::header_size)
        return {};
    auto body = std::span<const std::byte>{frame}.subspan(wire::header_size);
    return std::string{reinterpret_cast<const char *>(body.data()), body.size()};
}

// A valid frame header that claims payload_len bytes, with NO payload following —
// the slowloris shape: the reassembler buffers the header + waits for a payload
// that never comes, so the no-progress deadline fires.
std::array<std::byte, wire::header_size> withholding_header(std::uint64_t payload_len)
{
    wire::frame_header hdr{};
    hdr.type = wire::msg_type::unidirectional;
    hdr.flags = 0;
    hdr.session_id = 0;
    hdr.timestamp_ns = 0;
    hdr.payload_len = payload_len;
    return wire::encode_header(hdr);
}

}

TEST_CASE("asio stream channel: a TCP channel applies the socket-option overrides; the default leaves the kernel untouched; a unix channel no-ops",
          "[integration][asio][hardening][socket-options]")
{
    // A connected loopback pair adopted into accept-mode channels: a configured channel
    // applies SO_SNDBUF/SO_RCVBUF + SO_KEEPALIVE so a get_option readback MOVES off the
    // default (the kernel may double or clamp, so the assertion is "moved", not "equals"),
    // while a default-constructed channel leaves the buffers untouched and keepalive off.
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};

    auto connected_pair = [&] {
        ::asio::ip::tcp::socket peer{io};
        ::asio::ip::tcp::socket client{io};
        client.connect(acc.local_endpoint());
        acc.accept(peer);
        return std::pair{std::move(peer), std::move(client)};
    };

    // The kernel default a fresh socket carries, read before any override — the floor the
    // configured channel must move off of.
    int default_snd = 0;
    int default_rcv = 0;
    {
        auto [peer, client] = connected_pair();
        ::asio::socket_base::send_buffer_size snd;
        ::asio::socket_base::receive_buffer_size rcv;
        client.get_option(snd);
        client.get_option(rcv);
        default_snd = snd.value();
        default_rcv = rcv.value();
    }

    // A configured channel: a buffer override + keepalive on. The readback asserts the
    // buffers MOVED off the default (the override is half the default so the move is
    // unambiguous in EITHER direction — the kernel may double or clamp, but a value equal to
    // the default would mean the knob never applied) and keepalive reads back true.
    {
        auto [peer, client] = connected_pair();
        pasio::stream_socket_options opts;
        opts.so_sndbuf = static_cast<std::size_t>(default_snd) / 2;
        opts.so_rcvbuf = static_cast<std::size_t>(default_rcv) / 2;
        opts.keepalive = true;
        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{},
                               pio::congestion::block, pasio::asio_channel::default_write_queue_bytes,
                               opts};

        ::asio::socket_base::send_buffer_size snd;
        ::asio::socket_base::receive_buffer_size rcv;
        ::asio::socket_base::keep_alive ka;
        ch.socket().get_option(snd);
        ch.socket().get_option(rcv);
        ch.socket().get_option(ka);
        REQUIRE(snd.value() != default_snd);   // moved off the kernel default
        REQUIRE(rcv.value() != default_rcv);
        REQUIRE(ka.value() == true);
        ch.close();
    }

    // A default channel: the buffers stay at the kernel default and keepalive stays off.
    {
        auto [peer, client] = connected_pair();
        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{},
                               pio::congestion::block};

        ::asio::socket_base::send_buffer_size snd;
        ::asio::socket_base::receive_buffer_size rcv;
        ::asio::socket_base::keep_alive ka;
        ch.socket().get_option(snd);
        ch.socket().get_option(rcv);
        ch.socket().get_option(ka);
        REQUIRE(snd.value() == default_snd);   // untouched
        REQUIRE(rcv.value() == default_rcv);
        REQUIRE(ka.value() == false);
        ch.close();
    }
}

TEST_CASE("asio stream channel: a bad-magic byte run fires on_protocol_close(invalid_magic)",
          "[integration][asio][hardening]")
{
    constexpr int k_iterations = 20;
    int proven = 0;
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

TEST_CASE("asio stream channel: a header with a withheld payload fires on_protocol_close(no_progress_timeout)",
          "[integration][asio][hardening]")
{
    constexpr int k_iterations = 20;
    int proven = 0;
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

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

pio::handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return pio::handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                     .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A peer_session over a real accepted TCP channel, built dialer-style so its
// on_transport_drop is wired (the re-dial seam). A raw client socket is the
// misbehaving peer. The harness counts on_transport_drop firings: a FRAMING
// protocol-close must NOT fire it (the dial-rearm short-circuit), while a real
// socket drop MUST. The handshake timeout is an hour so it never resolves the leg.
// Member order: io BEFORE the session so destruction unwinds the session first.
struct session_under_peer
{
    using asio_policy = pasio::asio_policy;
    using session = pio::peer_session<asio_policy>;
    using msg_forwarder = pio::message_forwarder<asio_policy>;
    using rpc_forwarder = pio::procedure_forwarder<asio_policy>;

    ::asio::io_context io;
    pasio::asio_listener listener{io, short_cfg()};
    ::asio::ip::tcp::socket client{io};

    msg_forwarder messages{};
    rpc_forwarder procedures{io, k_long_timeout};
    pio::peer_context<asio_policy> ctx;
    std::optional<session> peer;

    int drops{0};

    session_under_peer()
    {
        listener.on_accepted([this](std::unique_ptr<pasio::asio_channel> ch) {
            ctx.channel = std::move(ch);
            ctx.node_name = "raw-peer";
            peer.emplace(ctx, io, make_cfg(0x02), k_long_timeout, messages, procedures, false);
            peer->start();
            peer->on_transport_drop([this] { ++drops; });
        });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.connect(ep);
        pump_until([this] { return peer.has_value(); });
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(50))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void write_raw(std::span<const std::byte> bytes)
    {
        ::asio::write(client, ::asio::buffer(bytes.data(), bytes.size()));
    }
};

}

TEST_CASE("asio stream channel: the bounded write queue sheds under congestion=drop and stalls under block",
          "[integration][asio][hardening]")
{
    // The byte-bounded write-queue edge proven over a real (connected) TCP socket whose
    // PEER NEVER READS, so the kernel send buffer fills and async_write stalls — the
    // userspace write queue then accumulates and the byte cap engages deterministically.
    // A small cap (4 KiB) bounds the queue; a stream of 1 KiB frames fills it, after which:
    //   * congestion=drop sheds the overrun at the publisher (dropped_count advances), and
    //   * congestion=block surfaces would_block (the stall edge) and sheds nothing.
    // The queue NEVER grows past the cap (backpressured() stays <= cap).
    //
    // The stall is forced STRUCTURALLY, not by out-pacing the kernel: both endpoints'
    // socket buffers are pinned to the floor (SO_SNDBUF on the channel end, SO_RCVBUF on
    // the never-reading peer) BEFORE adoption, so a few KiB in flight stalls async_write
    // outright. Without this the test depends on writing megabytes faster than the serial
    // drain empties them — a race that asan instrumentation loses (the drain keeps pace,
    // the queue never reaches the cap, and the shed/stall edge never fires).
    auto run = [](pio::congestion mode) {
        ::asio::io_context io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket peer{io};
        ::asio::ip::tcp::socket client{io};
        client.connect(acc.local_endpoint());
        acc.accept(peer);                               // peer adopts but NEVER reads

        // Pin both kernel buffers to the floor so async_write stalls after a few KiB, not
        // after megabytes. The kernel may round up, but to its own minimum (KiB-scale),
        // never to the default 100s of KiB — enough headroom collapses that the 4 KiB
        // userspace cap engages deterministically regardless of producer/drain timing.
        client.set_option(::asio::socket_base::send_buffer_size{1024});
        peer.set_option(::asio::socket_base::receive_buffer_size{1024});

        constexpr std::size_t cap = 4096;
        // Adopt the connected client end into an accept-mode channel with the small cap.
        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{}, mode, cap};

        std::optional<pio::io_error> err;
        ch.on_error([&](pio::io_error e) { err = e; });

        // Send 1 KiB frames until the byte cap engages (peer not reading -> buffer fills).
        // With the floored buffers the stall arrives in the first handful of frames; the
        // loop bound is a generous safety ceiling, not a timing dependency.
        std::vector<std::byte> kib(1024, std::byte{0x5A});
        for(int i = 0; i < 4096; ++i)
        {
            ch.send(kib);
            io.poll();                                  // let async_write make what progress it can
            REQUIRE(ch.backpressured() <= cap);         // NEVER grows past the cap
            if(mode == pio::congestion::drop_newest && ch.dropped_count() > 0)
                break;
            if(mode == pio::congestion::block && err.has_value())
                break;
        }

        if(mode == pio::congestion::drop_newest)
        {
            REQUIRE(ch.dropped_count() > 0);            // the overrun was shed at the publisher
        }
        else
        {
            REQUIRE(err.has_value());
            REQUIRE(*err == pio::io_error::would_block);   // block stalls, never grows
            REQUIRE(ch.dropped_count() == 0);              // block sheds nothing
        }
        REQUIRE(ch.backpressured() <= cap);
    };

    run(pio::congestion::drop_newest);
    run(pio::congestion::block);
}

TEST_CASE("asio stream channel: a sustained multi-frame burst gathers into coalesced writes and delivers every frame intact under asan",
          "[integration][asio][hardening][throughput]")
{
    // The owner-lifetime proof under a throughput cell. A real loopback pair: the
    // client channel sends a sustained burst of distinct frames in tight bursts so the
    // serial send queue accumulates a backlog that the gather-write coalesces into ONE
    // writev per drain turn. The single completion fires for N frames, so every gathered
    // node's owner MUST outlive it — under build-asan a freed owner read by the writev is
    // a heap-use-after-free and a leaked node is a leak. The proof is the throughput cell
    // running clean AND every distinct frame arriving intact and in order (a coalesced run
    // that dropped/aliased an owner would corrupt or lose a frame). Looped for
    // reproducibility (a transport claim is never made from one run).
    constexpr int k_iterations = 8;
    constexpr int k_frames = 2000;            // a sustained cell: deep enough to force gathering
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket raw_server{io};
        ::asio::ip::tcp::socket raw_client{io};
        raw_client.connect(acc.local_endpoint());
        acc.accept(raw_server);

        // A generous cap so the burst backlog accumulates (the gather has frames to coalesce)
        // without the cap shedding; congestion=block surfaces nothing while the peer reads.
        constexpr std::size_t cap = 64u * 1024u * 1024u;
        pasio::asio_channel server{io, std::move(raw_server), wire::stream_inbound_config{},
                                   pio::congestion::block, cap};
        pasio::asio_channel client{io, std::move(raw_client), wire::stream_inbound_config{},
                                   pio::congestion::block, cap};

        std::vector<std::vector<std::byte>> received;
        server.on_data([&](std::span<const std::byte> d) { received.emplace_back(d.begin(), d.end()); });

        // Distinct framed payloads: each frame's body encodes its index so an out-of-order
        // or corrupted gather is caught, not just a count mismatch.
        auto frame_for = [](int i) {
            wire::frame_header hdr{};
            hdr.type = wire::msg_type::unidirectional;
            const std::string body = "frame-" + std::to_string(i);
            std::vector<std::byte> payload(body.size());
            std::memcpy(payload.data(), body.data(), body.size());
            hdr.payload_len = payload.size();
            std::vector<std::byte> out;
            auto h = wire::encode_header(hdr);
            out.insert(out.end(), h.begin(), h.end());
            out.insert(out.end(), payload.begin(), payload.end());
            return out;
        };

        // Send in tight sub-bursts (no pump between) so the send queue backs up and the
        // drive() gather coalesces a multi-frame writev; pump only between sub-bursts.
        int sent = 0;
        while(sent < k_frames)
        {
            for(int b = 0; b < 64 && sent < k_frames; ++b, ++sent)
                client.send(frame_for(sent));
            io.poll();
        }

        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while(static_cast<int>(received.size()) < k_frames
              && std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE(static_cast<int>(received.size()) == k_frames);   // every frame delivered, none lost to a freed owner
        for(int i = 0; i < k_frames; ++i)
        {
            const std::string body = data_body(received[i]);
            REQUIRE(body == "frame-" + std::to_string(i));        // intact and in FIFO order
        }

        client.close();
        server.close();
        io.poll();
    }
}

TEST_CASE("asio stream channel: a FRAMING protocol-close does NOT re-dial while a real socket drop does",
          "[integration][asio][hardening]")
{
    constexpr int k_iterations = 20;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // Framing close: a header-size+ bad-magic run trips invalid_magic in the
        // accepted channel's stream_inbound -> on_protocol_close ->
        // close_for_protocol_error. The latch short-circuits the re-dial seam, so
        // on_transport_drop never fires: drops stays 0.
        {
            session_under_peer h;
            REQUIRE(h.peer.has_value());
            std::array<std::byte, wire::header_size + 4> garbage{};
            garbage.fill(std::byte{0xFF});
            h.write_raw(garbage);
            h.settle();
            REQUIRE(h.drops == 0);
            REQUIRE(!h.peer->is_complete());
        }

        // Real transport drop: the raw client closes its socket. The accepted
        // channel's read loop surfaces a network error -> on_error -> the (not
        // latched) re-dial seam fires: drops advances to 1.
        {
            session_under_peer h;
            REQUIRE(h.peer.has_value());
            h.client.close();
            h.pump_until([&] { return h.drops >= 1; });
            REQUIRE(h.drops == 1);
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
