#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"

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

namespace {

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

// The outcome of one late-join scenario over real TCP loopback. `replayed` is
// what the LATE subscriber received within a bounded grace window right after it
// subscribed (the retained value on a latched topic; nothing on a non-latched
// one). `after_live` is what it received from a subsequent live publish (proves
// the channel itself works — only the replay is conditional on the latch).
struct late_join
{
    std::optional<std::vector<std::byte>> replayed;
    std::optional<std::vector<std::byte>> after_live;
};

// Run one full late-join scenario over real TCP loopback. A publisher-side
// forwarder fans toward an accepted server channel; a SECOND client connects and
// subscribes (attach_for_fanout) and routes every received frame through a real
// frame_router — the same demux any live frame traverses, so a replayed frame is
// recovered exactly as a normal data frame. Each call stands up a fresh
// listener + client + io_context so the iterations are independent.
late_join one_late_join(std::span<const std::byte> payload, std::string_view fqn, bool latched)
{
    ::asio::io_context io;

    pasio::asio_listener listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch)
    {
        server_channel = std::move(ch);
    });
    listener.start({"tcp", "127.0.0.1:0"});
    auto port = listener.port();

    // The late subscriber: it routes received frames through a frame_router whose
    // on_unidirectional decodes the inner payload — the production receive contract.
    pasio::asio_channel client(io);
    std::optional<std::vector<std::byte>> received;
    pio::frame_router router;
    router.on_unidirectional([&](std::span<const std::byte> inner)
    {
        auto decoded = wire::decode_unidirectional(inner);
        if(decoded)
            received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
    });
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    while(!server_channel)
        io.poll_one();

    // Latch then publish BEFORE the late subscribe: retention happens on any
    // latched publish regardless of subscriber count, so the value is retained
    // here with zero subscribers (a non-latched publish with no subscriber is a
    // no-op — demand-driven). The MEASURED second client then subscribes and its
    // attach_for_fanout drives the replay of the retained frame.
    pio::message_forwarder<pasio::asio_policy> fwd;
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "late-node"};
    if(latched)
        fwd.latch(fqn);
    fwd.publish(fqn, payload);

    fwd.attach_for_fanout(sub, fqn);

    // Bounded grace: a replay over loopback arrives in microseconds, so a 50ms
    // drained window is ample to catch it and to prove its ABSENCE on a
    // non-latched topic without ever hanging.
    auto grace = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while(!received && std::chrono::steady_clock::now() < grace)
        io.poll();

    late_join out;
    out.replayed = received;

    // A live publish now fans out to the subscribed client in both cases — the
    // channel works; only the replay was conditional on the latch.
    received.reset();
    fwd.publish(fqn, payload);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(!received && std::chrono::steady_clock::now() < deadline)
        io.poll();
    out.after_live = received;

    return out;
}

}

TEST_CASE("asio latch replay delivers a late client the retained value over real TCP, looped",
          "[integration][latch][asio]")
{
    const auto payload = bytes_of("plexus-latched-retained-value");
    const std::string fqn = "demo._plexus._tcp.local.";

    // Loop the FULL live roundtrip >=100 times in-body: a live networking claim is
    // proven reproducible, never from a single run. Each iteration is an
    // independent listener+client+io_context, so a flaky frame surfaces as a
    // mismatch on some iteration rather than a one-off pass.
    constexpr int k_iterations = 100;
    int delivered = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto out = one_late_join(payload, fqn, /*latched=*/true);
        REQUIRE(out.replayed.has_value());        // the retained value reached the late joiner
        REQUIRE(*out.replayed == payload);        // byte-identical to the published frame
        REQUIRE(out.after_live == payload);        // and live publishing still works
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("asio non-latched topic does not replay on a late subscribe, looped",
          "[integration][latch][asio]")
{
    const auto payload = bytes_of("plexus-live-only-no-replay");
    const std::string fqn = "demo._plexus._tcp.local.";

    // The non-latched NEGATIVE over real TCP (guards against an accidental
    // always-replay): the late subscriber receives NOTHING within the bounded
    // grace window, then a live publish IS received. Looped for reproducibility.
    constexpr int k_iterations = 100;
    int held = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto out = one_late_join(payload, fqn, /*latched=*/false);
        REQUIRE_FALSE(out.replayed.has_value());   // no replay on a non-latched topic
        REQUIRE(out.after_live == payload);         // but the next live publish arrives
        ++held;
    }
    REQUIRE(held == k_iterations);
}
