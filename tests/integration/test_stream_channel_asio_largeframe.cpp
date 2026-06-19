#include "test_stream_channel_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_asio_fixture;

TEST_CASE("asio stream channel: a 16 MB single frame round-trips byte-identically over a real TCP "
          "loopback pair, looped",
          "[integration][asio][envelope16]")
{
    // The lifted message-size envelope on the TCP stream path: one 16 MiB frame written end
    // to end over a real loopback pair and reassembled byte-equal. The receive channel's
    // inbound config raises max_payload_size + buffered_bytes_cap above the 16 MiB payload
    // (the reassembler ceiling defends the subscriber's own memory); the send channel's
    // write-queue byte cap is raised to hold the whole framed message under congestion=block.
    // Looped in-body and re-run across process runs (a transport claim is never made from one
    // run); the frame body encodes a position ramp so a reorder or corruption is caught, not
    // just a size mismatch.
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
        ::asio::io_context        io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   raw_server{io};
        ::asio::ip::tcp::socket   raw_client{io};
        raw_client.connect(acc.local_endpoint());
        acc.accept(raw_server);

        pasio::asio_channel server{io, std::move(raw_server), cfg, pio::congestion::block,
                                   pio::egress_capacity::of_bytes(k_queue)};
        pasio::asio_channel client{io, std::move(raw_client), cfg, pio::congestion::block,
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

        // on_data delivers the complete frame (header || body); the reassembled bytes equal
        // the framed buffer, and the body past the fixed header equals the 16 MiB ramp.
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

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

pio::handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return pio::handshake_fsm_config{.self_id                  = id,
                                     .version_major            = 1,
                                     .version_minor            = 0,
                                     .compatible_version_major = 1,
                                     .compatible_version_minor = 0};
}

// A peer_session over a real accepted TCP channel, built dialer-style so its
// on_transport_drop is wired (the re-dial seam). A raw client socket is the
// misbehaving peer. The harness counts on_transport_drop firings: a FRAMING
// protocol-close must NOT fire it (the dial-rearm short-circuit), while a real
// socket drop MUST. The handshake timeout is an hour so it never resolves the leg.
// Member order: io BEFORE the session so destruction unwinds the session first.
struct session_under_peer
{
    using asio_policy   = pasio::asio_policy;
    using session       = pio::peer_session<asio_policy>;
    using msg_forwarder = pio::message_forwarder<asio_policy>;
    using rpc_forwarder = pio::procedure_forwarder<asio_policy>;

    ::asio::io_context      io;
    pasio::asio_listener    listener{io, short_cfg()};
    ::asio::ip::tcp::socket client{io};

    msg_forwarder                  messages{};
    rpc_forwarder                  procedures{io, k_long_timeout};
    pio::peer_context<asio_policy> ctx;
    std::optional<session>         peer;

    int drops{0};

    session_under_peer()
    {
        listener.on_accepted(
                [this](std::unique_ptr<pasio::asio_channel> ch)
                {
                    ctx.channel   = std::move(ch);
                    ctx.node_name = "raw-peer";
                    peer.emplace(ctx, io, make_cfg(0x02), k_long_timeout, messages, procedures,
                                 false);
                    peer->start();
                    peer->on_transport_drop([this] { ++drops; });
                });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.connect(ep);
        pump_until([this] { return peer.has_value(); });
    }

    template<typename Pred>
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

TEST_CASE("asio stream channel: a FRAMING protocol-close does NOT re-dial while a real socket drop "
          "does",
          "[integration][asio][hardening]")
{
    constexpr int k_iterations = 20;
    int           proven       = 0;
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
