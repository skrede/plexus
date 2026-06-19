#include "test_stream_channel_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_unix_fixture;

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
