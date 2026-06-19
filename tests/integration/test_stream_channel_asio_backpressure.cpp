#include "test_stream_channel_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_asio_fixture;

TEST_CASE("asio stream channel: the bounded write queue sheds under congestion=drop and stalls "
          "under block",
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
    auto run = [](pio::congestion mode)
    {
        ::asio::io_context        io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   peer{io};
        ::asio::ip::tcp::socket   client{io};
        client.connect(acc.local_endpoint());
        acc.accept(peer); // peer adopts but NEVER reads

        // Pin both kernel buffers to the floor so async_write stalls after a few KiB, not
        // after megabytes. The kernel may round up, but to its own minimum (KiB-scale),
        // never to the default 100s of KiB — enough headroom collapses that the 4 KiB
        // userspace cap engages deterministically regardless of producer/drain timing.
        client.set_option(::asio::socket_base::send_buffer_size{1024});
        peer.set_option(::asio::socket_base::receive_buffer_size{1024});

        constexpr std::size_t cap = 4096;
        // Adopt the connected client end into an accept-mode channel with the small cap.
        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{}, mode,
                               pio::egress_capacity::of_bytes(cap)};

        std::optional<pio::io_error> err;
        ch.on_error([&](pio::io_error e) { err = e; });

        // Send 1 KiB frames until the byte cap engages (peer not reading -> buffer fills).
        // With the floored buffers the stall arrives in the first handful of frames; the
        // loop bound is a generous safety ceiling, not a timing dependency.
        std::vector<std::byte> kib(1024, std::byte{0x5A});
        for(int i = 0; i < 4096; ++i)
        {
            ch.send(kib);
            io.poll();                          // let async_write make what progress it can
            REQUIRE(ch.backpressured() <= cap); // NEVER grows past the cap
            if(mode == pio::congestion::drop_newest && ch.dropped_count() > 0)
                break;
            if(mode == pio::congestion::block && err.has_value())
                break;
        }

        if(mode == pio::congestion::drop_newest)
        {
            REQUIRE(ch.dropped_count() > 0); // the overrun was shed at the publisher
        }
        else
        {
            REQUIRE(err.has_value());
            REQUIRE(*err == pio::io_error::would_block); // block stalls, never grows
            REQUIRE(ch.dropped_count() == 0);            // block sheds nothing
        }
        REQUIRE(ch.backpressured() <= cap);
    };

    run(pio::congestion::drop_newest);
    run(pio::congestion::block);
}

TEST_CASE("asio stream channel: a sustained multi-frame burst gathers into coalesced writes and "
          "delivers every frame intact under asan",
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
    constexpr int k_frames     = 2000; // a sustained cell: deep enough to force gathering
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context        io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   raw_server{io};
        ::asio::ip::tcp::socket   raw_client{io};
        raw_client.connect(acc.local_endpoint());
        acc.accept(raw_server);

        // A generous cap so the burst backlog accumulates (the gather has frames to coalesce)
        // without the cap shedding; congestion=block surfaces nothing while the peer reads.
        constexpr std::size_t cap = 64u * 1024u * 1024u;
        pasio::asio_channel   server{io, std::move(raw_server), wire::stream_inbound_config{},
                                     pio::congestion::block, pio::egress_capacity::of_bytes(cap)};
        pasio::asio_channel   client{io, std::move(raw_client), wire::stream_inbound_config{},
                                     pio::congestion::block, pio::egress_capacity::of_bytes(cap)};

        std::vector<std::vector<std::byte>> received;
        server.on_data([&](std::span<const std::byte> d)
                       { received.emplace_back(d.begin(), d.end()); });

        // Distinct framed payloads: each frame's body encodes its index so an out-of-order
        // or corrupted gather is caught, not just a count mismatch.
        auto frame_for = [](int i)
        {
            wire::frame_header hdr{};
            hdr.type                    = wire::msg_type::unidirectional;
            const std::string      body = "frame-" + std::to_string(i);
            std::vector<std::byte> payload(body.size());
            std::memcpy(payload.data(), body.data(), body.size());
            hdr.payload_len = payload.size();
            std::vector<std::byte> out;
            auto                   h = wire::encode_header(hdr);
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
        while(static_cast<int>(received.size()) < k_frames &&
              std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE(static_cast<int>(received.size()) ==
                k_frames); // every frame delivered, none lost to a freed owner
        for(int i = 0; i < k_frames; ++i)
        {
            const std::string body = data_body(received[i]);
            REQUIRE(body == "frame-" + std::to_string(i)); // intact and in FIFO order
        }

        client.close();
        server.close();
        io.poll();
    }
}
