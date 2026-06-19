#include "test_stream_channel_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_unix_fixture;

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
