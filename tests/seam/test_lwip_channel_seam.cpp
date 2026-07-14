// Host slice for the constrained-target lwIP byte_channel + dial transport: the two static_asserts
// fire at compile time (the load-bearing proof that lwip_channel satisfies byte_channel and
// lwip_transport satisfies transport_backend), and the runtime TEST_CASEs round-trip a framed
// message over a REAL localhost-TCP loopback pair (a genuine POSIX stream_socket, not a mock) — so
// real partial reads / EWOULDBLOCK + real stream_inbound framing + the P1 poll-drive + the dial path
// are exercised end-to-end with no hardware. This TU is additive to the host suite baseline.

#include "host_tcp_socket.h" // MUST precede lwip_transport.h: declares the host POSIX stream_socket
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
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

// The explicit witnesses at the test site (mirror the gates in the headers).
static_assert(plexus::io::byte_channel<lwip_channel>, "lwip_channel must satisfy byte_channel at the seam-test site");
static_assert(plexus::io::transport_backend<plexus::freertos::lwip_transport<host_tcp_socket>, plexus::freertos::lwip_policy<host_tcp_socket>>,
              "lwip_transport must satisfy transport_backend at the seam-test site");

namespace {

// The complete header-on frame a peer puts on a TCP link: header + payload, NO CRC trailer (TCP
// provides integrity — the CRC layer is UART-only).
std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
}

std::vector<std::byte> bytes_of(std::initializer_list<int> vs)
{
    std::vector<std::byte> out;
    for(int v : vs)
        out.push_back(std::byte(v));
    return out;
}

lwip_channel make_channel(host_tcp_socket socket, plexus::freertos::freertos_executor &ex)
{
    return lwip_channel{std::move(socket), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}};
}

}

TEST_CASE("lwip_channel round-trips a framed message over a real loopback TCP pair", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    plexus::freertos::freertos_executor ex;
    auto sender   = make_channel(std::move(pair->dialed), ex);
    auto receiver = make_channel(std::move(pair->accepted), ex);

    std::vector<std::byte> received;
    receiver.on_data([&](std::span<const std::byte> frame) { received.assign(frame.begin(), frame.end()); });

    const auto payload = bytes_of({0xDE, 0xAD, 0xBE, 0xEF});
    sender.send(on_wire(payload));

    plexus::test::poll_until([&] { receiver.poll(); }, [&] { return !(received.empty()); });

    REQUIRE(received.size() == plexus::wire::header_size + payload.size());
    const auto inner = std::span<const std::byte>{received}.subspan(plexus::wire::header_size);
    REQUIRE(std::vector<std::byte>(inner.begin(), inner.end()) == payload);
}

TEST_CASE("lwip_channel reassembles a frame split across two real recv turns", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    plexus::freertos::freertos_executor ex;
    host_tcp_socket raw_sender = std::move(pair->dialed);
    auto receiver              = make_channel(std::move(pair->accepted), ex);

    int deliveries = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++deliveries; });

    const auto payload = bytes_of({0x01, 0x02, 0x03, 0x04, 0x05});
    const auto frame   = on_wire(payload);
    const auto head    = std::span<const std::byte>{frame}.first(plexus::wire::header_size);
    const auto tail    = std::span<const std::byte>{frame}.subspan(plexus::wire::header_size);

    raw_sender.send(head);
    for(int turn = 0; turn < 50; ++turn)
        receiver.poll();
    REQUIRE(deliveries == 0); // header alone is a partial frame — nothing delivered yet

    raw_sender.send(tail);
    plexus::test::poll_until([&] { receiver.poll(); }, [&] { return !(deliveries == 0); });

    REQUIRE(deliveries == 1); // the split frame reassembled into exactly one clean delivery
}

TEST_CASE("lwip_channel surfaces a peer close as on_error, never on_protocol_close", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    plexus::freertos::freertos_executor ex;
    host_tcp_socket sender = std::move(pair->dialed);
    auto receiver          = make_channel(std::move(pair->accepted), ex);

    int errors          = 0;
    bool protocol_close = false;
    receiver.on_error([&](plexus::io::io_error) { ++errors; });
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_close = true; });

    sender.close(); // an orderly peer FIN mid-stream

    plexus::test::poll_until([&] { receiver.poll(); }, [&] { return !(errors == 0); });

    REQUIRE(errors == 1);             // the hard-drop seam fired
    REQUIRE_FALSE(protocol_close);    // DISTINCT from the framing-violation seam
}

TEST_CASE("lwip_transport dials a real loopback listener and delivers one channel", "[seam]")
{
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(listener >= 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len        = sizeof(addr);
    REQUIRE(::bind(listener, reinterpret_cast<sockaddr *>(&addr), len) == 0);
    REQUIRE(::listen(listener, 1) == 0);
    REQUIRE(::getsockname(listener, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
    const std::string address = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));

    plexus::freertos::freertos_executor ex;
    plexus::freertos::lwip_transport<host_tcp_socket> transport{ex};

    std::unique_ptr<lwip_channel> dialed;
    plexus::io::endpoint           dialed_ep;
    transport.on_dialed([&](std::unique_ptr<lwip_channel> ch, const plexus::io::endpoint &ep)
                        {
                            dialed    = std::move(ch);
                            dialed_ep = ep;
                        });

    const plexus::io::endpoint target{"tcp", address};
    transport.dial(target);

    REQUIRE(dialed != nullptr);     // the dial produced exactly one channel
    REQUIRE(dialed_ep == target);   // the endpoint rode back as the correlation key
    ::close(::accept(listener, nullptr, nullptr));
    ::close(listener);
}

TEST_CASE("lwip_transport reports a refused dial through on_dial_failed with the endpoint", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    plexus::freertos::lwip_transport<host_tcp_socket> transport{ex};

    plexus::io::endpoint failed_ep;
    bool                 failed = false;
    transport.on_dial_failed([&](const plexus::io::endpoint &ep, plexus::io::io_error)
                             {
                                 failed    = true;
                                 failed_ep = ep;
                             });

    const plexus::io::endpoint target{"tcp", "127.0.0.1:1"}; // nothing listens on port 1
    transport.dial(target);

    REQUIRE(failed);
    REQUIRE(failed_ep == target); // the failed endpoint rode back as the correlation key
}
