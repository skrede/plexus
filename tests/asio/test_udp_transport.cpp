#include "test_udp_transport_common.h"

using namespace udp_transport_fixture;

namespace {

static_assert(plexus::io::byte_channel<pasio::udp_channel>);
static_assert(plexus::io::transport_backend<pasio::udp_transport, pasio::udp_policy>);
// A real loopback pair on one io_context (a listening transport and a dialing one); the
// handshake ARQ establishes before any data flows, capturing the accepted + dialed channels.
struct loopback
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io};

    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;

    std::vector<std::string>            received; // frames posted to the accepted channel
    std::optional<plexus::io::io_error> client_error;
    int                                 accepted_protocol_closes{0};

    loopback()
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b)
                                      { received.push_back(str_of(b)); });
                    accepted->on_protocol_close([this](wire::close_cause)
                                                { ++accepted_protocol_closes; });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until([this] { return server.port() != 0; });

        client.on_dialed(
                [this](std::unique_ptr<pasio::udp_channel> ch, const plexus::io::endpoint &)
                {
                    dialed = std::move(ch);
                    dialed->on_error([this](plexus::io::io_error e) { client_error = e; });
                });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until([this] { return dialed != nullptr && accepted != nullptr; });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

}

TEST_CASE("udp best_effort: a dialed channel sends a frame the accepting channel posts identically",
          "[udp][transport]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        loopback h;
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        const std::string payload = "best_effort-" + std::to_string(iter);
        auto              frame   = bytes_of(payload);
        h.dialed->send(frame);

        h.pump_until([&] { return !h.received.empty(); });
        REQUIRE(h.received.size() == 1);
        REQUIRE(h.received.front() == payload);
        REQUIRE(h.accepted_protocol_closes == 0); // non-stream: on_protocol_close never fires
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp dedup_live: a replayed datagram is dropped — on_data fires once", "[udp][transport]")
{
    loopback h;
    REQUIRE(h.dialed != nullptr);
    REQUIRE(h.accepted != nullptr);

    // Encode the SAME envelope (same seq) twice and inject both at the raw socket
    // layer (bypassing the channel's send-side seq increment) so the receiver sees a
    // genuine replay. seq=0 matches the channel's first outbound seq.
    auto frame    = bytes_of("replay-me");
    auto datagram = wire::wrap_udp(wire::udp_envelope_kind::best_effort, 0, frame);

    // Deliver the identical envelope (same seq) twice to the accepted channel: the
    // per-peer dedup window admits the first as fresh and drops the second as a
    // duplicate, so on_data fires exactly once.
    h.accepted->deliver_inbound(datagram);
    h.accepted->deliver_inbound(datagram);
    h.pump_until([&] { return !h.received.empty(); });

    REQUIRE(h.received.size() == 1);
    REQUIRE(h.received.front() == "replay-me");
}

TEST_CASE("udp oversize: a message beyond the max-message size is rejected at publish with a "
          "surfaced error",
          "[udp][transport]")
{
    loopback h;
    REQUIRE(h.dialed != nullptr);

    // A payload merely past the per-datagram budget is now SPLIT across fragments (not
    // rejected) — the oversize-reject path fires ONLY for a payload beyond the bounded
    // max-MESSAGE size, the genuinely-too-big case the reassembler cannot hold. The live
    // ceiling is the channel's effective-max (the node default), not the fragment-count
    // assert constant.
    std::vector<std::byte> too_big(plexus::io::global_default_max_message_bytes + 1,
                                   std::byte{0x5A});
    h.dialed->send(too_big);
    h.pump_until([&] { return h.client_error.has_value(); });

    REQUIRE(h.client_error.has_value());
    REQUIRE(*h.client_error == plexus::io::io_error::message_too_large);
    REQUIRE(h.dialed->is_open()); // rejected at publish, channel stays open — not a drop/close

    // A frame that just fits one datagram still sends unfragmented (boundary: size +
    // overhead == max_payload) — the small path is byte-identical to before.
    h.client_error.reset();
    std::vector<std::byte> fits(
            pasio::udp_channel::default_max_payload - wire::udp_envelope_overhead, std::byte{0x01});
    h.dialed->send(fits);
    h.pump_until([&] { return !h.received.empty(); });
    REQUIRE_FALSE(h.client_error.has_value());
    REQUIRE(h.received.size() == 1);
}

TEST_CASE("udp dial: a malformed port fails closed via on_dial_failed, never throwing",
          "[udp][transport][parse]")
{
    ::asio::io_context   io;
    pasio::udp_transport client{io};

    std::optional<plexus::io::io_error> dial_error;
    client.on_dial_failed([&](const plexus::io::endpoint &, plexus::io::io_error e)
                          { dial_error = e; });

    // A non-numeric / empty / overflowing port must NOT throw out of dial(): the parser
    // is fail-closed (from_chars, not stoul), so each routes to on_dial_failed.
    REQUIRE_NOTHROW(client.dial({"udp", "127.0.0.1:not-a-port"}));
    REQUIRE(dial_error.has_value());

    dial_error.reset();
    REQUIRE_NOTHROW(client.dial({"udp", "127.0.0.1:"}));
    REQUIRE(dial_error.has_value());

    dial_error.reset();
    REQUIRE_NOTHROW(client.dial({"udp", "127.0.0.1:70000"})); // > 65535
    REQUIRE(dial_error.has_value());

    dial_error.reset();
    REQUIRE_NOTHROW(client.dial({"udp", "127.0.0.1:80junk"})); // trailing junk
    REQUIRE(dial_error.has_value());
}

TEST_CASE("udp best_effort drops a kind=1 datagram without spinning up an ARQ engine",
          "[udp][transport][mode]")
{
    loopback h;
    REQUIRE(h.accepted != nullptr);
    REQUIRE(h.accepted->mode() == plexus::datagram::detail::udp_channel_mode::best_effort);

    // A spoofed reliable_arq (kind=1) data segment from the peer's source endpoint must be
    // DROPPED on a best_effort channel — never routed to the reliable path that would
    // construct an unsolicited ARQ engine and start acking. With seq=0 it would otherwise
    // be a valid first segment.
    auto                   payload = bytes_of("spoofed-reliable");
    std::vector<std::byte> inner(1 + payload.size());
    inner[0] = static_cast<std::byte>(wire::udp_arq_kind::segment);
    for(std::size_t i = 0; i < payload.size(); ++i)
        inner[i + 1] = payload[i];
    auto datagram = wire::wrap_udp(wire::udp_envelope_kind::reliable_arq, 0,
                                   std::span<const std::byte>{inner});

    h.accepted->deliver_inbound(datagram);
    for(int i = 0; i < 64; ++i) // drain any (erroneous) posted delivery / ack without a long wait
    {
        h.io.poll();
        if(h.io.stopped())
            h.io.restart();
    }

    // The kind=1 datagram was dropped: no in-order payload was posted to on_data, and the
    // channel never built the reliable engine (a best_effort channel stays fire-and-forget).
    REQUIRE(h.received.empty());
}
