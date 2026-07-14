// Host slice for the constrained-target lwIP listen/accept transport: drives the server half over a
// REAL localhost-TCP loopback (a genuine POSIX listener test-side, no hardware and no mock). The TU
// proves accept -> on_accepted -> a poll-able channel, the compile-time accept cap bound (default 1
// refuses the second client; cap 2 accepts both), and view invalidation on channel close (the
// on_closed wired in adopt() clears the view; asan-clean under the asan tree). Additive to the host
// suite baseline.

#include "host_tcp_listener.h" // MUST precede lwip_transport.h: declares the host POSIX listener
#include "host_tcp_socket.h"
#include "host_loopback_pair.h"

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <memory>
#include <cstddef>

using plexus::test::host_tcp_socket;
using plexus::test::host_tcp_listener;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

template<std::size_t MaxClients = 1>
using accept_transport = plexus::freertos::lwip_transport<host_tcp_socket, host_tcp_listener, MaxClients>;

static_assert(plexus::io::transport_backend<accept_transport<>, plexus::freertos::lwip_policy<host_tcp_socket>>,
              "the listen/accept transport must satisfy transport_backend at the seam-test site");

namespace {

std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
}

host_tcp_socket dial_loopback(const plexus::io::endpoint &ep)
{
    host_tcp_socket client;
    client.connect(ep);
    return client;
}

}

TEST_CASE("lwip_transport accepts a loopback client and delivers one poll-able channel", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    accept_transport<> transport{ex};
    host_tcp_listener  probe; // borrow the listener's bind to learn the ephemeral port the transport binds
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    const auto address = probe.local_endpoint();
    probe.close();
    transport.listen(address);

    std::unique_ptr<lwip_channel> accepted;
    transport.on_accepted([&](std::unique_ptr<lwip_channel> ch) { accepted = std::move(ch); });

    host_tcp_socket client = dial_loopback(address);
    plexus::test::poll_until([&] { transport.poll(); }, [&] { return !(!accepted); });
    REQUIRE(accepted != nullptr);

    std::vector<std::byte> received;
    accepted->on_data([&](std::span<const std::byte> frame) { received.assign(frame.begin(), frame.end()); });
    const std::byte payload[] = {std::byte{0xA5}, std::byte{0x5A}};
    client.send(on_wire(payload));
    plexus::test::poll_until([&] { transport.poll(); }, [&] { return !(received.empty()); });

    const auto inner = std::span<const std::byte>{received}.subspan(plexus::wire::header_size);
    REQUIRE(std::vector<std::byte>(inner.begin(), inner.end()) == std::vector<std::byte>(std::begin(payload), std::end(payload)));
}

TEST_CASE("lwip_transport accepts two clients under cap 2 and refuses a third under cap 1", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    accept_transport<2> two{ex};
    host_tcp_listener   probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    const auto address = probe.local_endpoint();
    probe.close();
    two.listen(address);

    int accepts = 0;
    std::vector<std::unique_ptr<lwip_channel>> kept;
    two.on_accepted([&](std::unique_ptr<lwip_channel> ch) { ++accepts; kept.push_back(std::move(ch)); });

    host_tcp_socket a = dial_loopback(address);
    host_tcp_socket b = dial_loopback(address);
    plexus::test::poll_until([&] { two.poll(); }, [&] { return !(accepts < 2); });
    REQUIRE(accepts == 2); // both clients delivered under cap 2

    plexus::freertos::freertos_executor ex1;
    accept_transport<1> one{ex1};
    host_tcp_listener   probe1;
    REQUIRE_FALSE(probe1.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    const auto address1 = probe1.local_endpoint();
    probe1.close();
    one.listen(address1);

    int                           accepts1 = 0;
    std::unique_ptr<lwip_channel> first;
    one.on_accepted([&](std::unique_ptr<lwip_channel> ch) { ++accepts1; if(!first) first = std::move(ch); });

    host_tcp_socket c = dial_loopback(address1);
    host_tcp_socket d = dial_loopback(address1);
    for(int turn = 0; turn < 200; ++turn)
        one.poll();
    REQUIRE(accepts1 == 1); // the bound holds: the second connection is refused, the set never grows
}

TEST_CASE("lwip_transport clears an accepted channel's view on close so poll never touches it", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    accept_transport<> transport{ex};
    host_tcp_listener  probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    const auto address = probe.local_endpoint();
    probe.close();
    transport.listen(address);

    std::unique_ptr<lwip_channel> accepted;
    transport.on_accepted([&](std::unique_ptr<lwip_channel> ch) { accepted = std::move(ch); });

    host_tcp_socket client = dial_loopback(address);
    plexus::test::poll_until([&] { transport.poll(); }, [&] { return !(!accepted); });
    REQUIRE(accepted != nullptr);

    accepted->close(); // the on_closed wired in adopt() clears the view before the channel is destroyed
    accepted.reset();
    for(int turn = 0; turn < 10; ++turn)
        transport.poll(); // poll-all must not touch the freed view (asan-clean is the proof)
    SUCCEED();
}
