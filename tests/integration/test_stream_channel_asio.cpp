#include "test_stream_channel_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_asio_fixture;

TEST_CASE("asio stream channel: a TCP channel applies the socket-option overrides; the default "
          "leaves the kernel untouched; a unix channel no-ops",
          "[integration][asio][hardening][socket-options]")
{
    // A connected loopback pair adopted into accept-mode channels: a configured channel
    // applies SO_SNDBUF/SO_RCVBUF + SO_KEEPALIVE so a get_option readback MOVES off the
    // default (the kernel may double or clamp, so the assertion is "moved", not "equals"),
    // while a default-constructed channel leaves the buffers untouched and keepalive off.
    ::asio::io_context        io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};

    auto connected_pair = [&]
    {
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
        ::asio::socket_base::send_buffer_size    snd;
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
        pasio::asio_channel ch{io,
                               std::move(client),
                               stream::stream_inbound_config{},
                               pio::congestion::block,
                               pio::egress_capacity::bounded_default(),
                               opts};

        ::asio::socket_base::send_buffer_size    snd;
        ::asio::socket_base::receive_buffer_size rcv;
        ::asio::socket_base::keep_alive          ka;
        ch.socket().get_option(snd);
        ch.socket().get_option(rcv);
        ch.socket().get_option(ka);
        REQUIRE(snd.value() != default_snd); // moved off the kernel default
        REQUIRE(rcv.value() != default_rcv);
        REQUIRE(ka.value() == true);
        ch.close();
    }

    // A default channel: the buffers stay at the kernel default and keepalive stays off.
    {
        auto [peer, client] = connected_pair();
        pasio::asio_channel ch{io, std::move(client), stream::stream_inbound_config{},
                               pio::congestion::block};

        ::asio::socket_base::send_buffer_size    snd;
        ::asio::socket_base::receive_buffer_size rcv;
        ::asio::socket_base::keep_alive          ka;
        ch.socket().get_option(snd);
        ch.socket().get_option(rcv);
        ch.socket().get_option(ka);
        REQUIRE(snd.value() == default_snd); // untouched
        REQUIRE(rcv.value() == default_rcv);
        REQUIRE(ka.value() == false);
        ch.close();
    }
}

TEST_CASE("asio stream channel: a bad-magic byte run fires on_protocol_close(invalid_magic)",
          "[integration][asio][hardening]")
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

TEST_CASE("asio stream channel: a header with a withheld payload fires "
          "on_protocol_close(no_progress_timeout)",
          "[integration][asio][hardening]")
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
