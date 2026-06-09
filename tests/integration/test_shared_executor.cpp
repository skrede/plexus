#include "plexus/mdnspp/mdnspp_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/data_frame.h"

#include "plexus/discovery/discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace wire = plexus::wire;
namespace pio = plexus::io;
namespace pmdns = plexus::mdnspp;

namespace {

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

}

// SLICE-2 locked requirement: mdnspp and the plexus asio transport share ONE
// io_context, driven on ONE thread. This test constructs an mdnspp_discovery AND
// the plexus asio channels from a single asio::io_context and pumps that single
// context with a bounded poll loop on the calling thread (no second context, no
// second thread). It asserts that BOTH a discovery browse and a transport
// round-trip make progress on that one context: the mDNS resolve callback is
// armed and the mDNS recv/timer handlers are driven by the same poll loop that
// carries the TCP accept/read/write completions, and the published bytes arrive
// intact over loopback.
//
// The host may resolve zero mDNS peers in a sandbox; the proof of the shared
// executor is structural — both objects bind `&io`, and the one poll loop drains
// both subsystems' handlers. The transport round-trip is the positive evidence
// that opaque bytes move on the same context the discovery machinery runs on.
TEST_CASE("mdnspp discovery and plexus asio transport progress on one io_context", "[integration][asio]")
{
    ::asio::io_context io;   // the SINGLE shared executor

    // --- Discovery on the shared context ---
    // Advertise a local service and browse for it on the SAME io_context, so a
    // real mDNS resolve (announce -> query -> aggregate) progresses on the shared
    // executor — positive evidence the discovery path runs on `io`, not merely
    // that its callback was armed.
    pmdns::mdnspp_discovery advertiser(io, "_plexus._tcp.local.");
    plexus::discovery::service_info local{"probe._plexus._tcp.local.", {"tcp", "127.0.0.1:5555"}};
    advertiser.advertise(local);

    pmdns::mdnspp_discovery discovery(io, "_plexus._tcp.local.");
    int resolved_count = 0;
    discovery.browse([&](const plexus::discovery::service_info &)
    {
        ++resolved_count;
    });

    // --- Transport round-trip on the SAME context ---
    pasio::asio_listener listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch)
    {
        server_channel = std::move(ch);
    });
    listener.start({"tcp", "127.0.0.1:0"});
    auto port = listener.port();

    pasio::asio_channel client(io);
    std::optional<std::vector<std::byte>> received;
    pio::frame_router router;
    router.on_unidirectional([&](const wire::frame_header &, std::span<const std::byte> inner)
    {
        auto decoded = wire::decode_unidirectional(inner);
        if(decoded)
            received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
    });
    // on_data now delivers a COMPLETE header-on frame; the frame_router owns the
    // header strip + type switch and hands the inner payload to its consumer.
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    // Single-threaded pump of the shared context until the accept completes.
    while(!server_channel)
        io.poll_one();

    const auto payload = bytes_of("shared-executor-payload");
    const std::string fqn = "demo._plexus._tcp.local.";
    pio::message_forwarder<pasio::asio_policy> fwd;
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "peer-node"};
    fwd.attach(sub, fqn);
    // The subscribe is now frame_header-wrapped (control frames are framed like
    // data), so it reassembles cleanly and the client's frame_router warn-and-
    // drops it (no subscribe consumer) while routing the data frame — no flush
    // loop is needed. The shared-executor proof is that this all rides one ctx.
    fwd.publish(fqn, payload);

    // Drive the ONE context: the SAME poll loop runs the TCP accept/read/write
    // completions AND the mDNS announce/query/recv/timer handlers — one
    // io_context carrying both subsystems. We run past the round-trip AND the
    // browse's silence timeout (mdnspp default 3s) so both subsystems progress to
    // a result on the shared context. Bounded so a regression fails fast.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while(std::chrono::steady_clock::now() < deadline &&
          (!received || resolved_count == 0))
        io.poll();

    discovery.stop();
    advertiser.stop();
    io.poll();   // drain the browse/advertise stop + trailing discovery handlers

    REQUIRE(received.has_value());
    REQUIRE(*received == payload);   // the transport round-trip progressed on `io`
    // The self-advertised service resolved through the mDNS path on the SAME
    // io_context — positive evidence the discovery subsystem ran on the shared
    // executor, not merely that its callback was armed.
    REQUIRE(resolved_count >= 1);
}
