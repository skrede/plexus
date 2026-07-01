// Host slice for the constrained-target lwIP transport's congestion plumbing: over a REAL localhost-TCP
// connection (a genuine POSIX stream_socket, no hardware and no mock) it proves the transport-level
// congestion policy reaches the channels it mints and that dropped() folds the shed total across its
// live views. drop_newest sheds + counts an over-cap send (transport.dropped() > 0); block backpressures
// the same send via would_block and never sheds (transport.dropped() == 0). Additive to the host suite.

#include "host_tcp_socket.h" // MUST precede lwip_transport.h: declares the host POSIX stream_socket

#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/congestion.h"
#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>
#include <optional>

namespace {

using plexus::test::host_tcp_socket;

// A test-owned stall flag bound at socket construction. The transport's dial() default-constructs its
// own socket internally, so the flag cannot be injected after the fact — binding it in the wrapper's
// ctor lets the loopback egress be held resident deterministically (the peer's timing cannot).
bool g_stall = false;

// A stream_socket that arms the stall flag on construction and forwards the rest to a host POSIX
// socket. The stall pointer rides host_tcp_socket's move, so it survives the transport's adopt().
class stalling_host_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    stalling_host_socket()
            : m_inner()
    {
        m_inner.use_stall_flag(g_stall);
    }

    stalling_host_socket(stalling_host_socket &&)            = default;
    stalling_host_socket &operator=(stalling_host_socket &&) = default;

    std::error_code connect(endpoint_type ep)
    {
        return m_inner.connect(std::move(ep));
    }
    std::size_t send(std::span<const std::byte> bytes)
    {
        return m_inner.send(bytes);
    }
    std::size_t recv(std::span<std::byte> buf)
    {
        return m_inner.recv(buf);
    }
    bool closed() const
    {
        return m_inner.closed();
    }
    void close()
    {
        m_inner.close();
    }
    void set_blocking(bool blocking)
    {
        m_inner.set_blocking(blocking);
    }

private:
    host_tcp_socket m_inner;
};

// A transient listener that stays OPEN so the transport can dial into it (make_loopback_pair closes
// its listener after one accept). The dialed connection completes on loopback without an accept, and
// the stall flag keeps any egress resident, so the backlog connection needs no drain.
struct open_listener
{
    int         fd;
    std::string address;
};

std::optional<open_listener> make_open_listener()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0)
        return std::nullopt;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    socklen_t len        = sizeof(addr);
    if(::bind(fd, reinterpret_cast<sockaddr *>(&addr), len) < 0 || ::listen(fd, 1) < 0 || ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) < 0)
    {
        ::close(fd);
        return std::nullopt;
    }
    return open_listener{fd, "127.0.0.1:" + std::to_string(ntohs(addr.sin_port))};
}

using transport_type = plexus::freertos::lwip_transport<stalling_host_socket>;
using channel_type   = transport_type::channel_type;
using lim            = plexus::freertos::lwip_channel_limits;

std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
}

std::vector<std::byte> filler(std::size_t n)
{
    return std::vector<std::byte>(n, std::byte{0x5A});
}

}

TEST_CASE("lwip_transport forwards drop_newest to its channels and folds the shed count", "[seam]")
{
    auto listener = make_open_listener();
    REQUIRE(listener.has_value());

    plexus::freertos::freertos_executor ex;
    const auto frame = on_wire(filler(64));
    transport_type transport(ex, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, frame.size() + 8, plexus::io::congestion::drop_newest);

    std::unique_ptr<channel_type> channel;
    transport.on_dialed([&](std::unique_ptr<channel_type> c, const plexus::io::endpoint &) { channel = std::move(c); });

    g_stall = true; // keep the first frame resident so the second overflows the cap
    transport.dial(plexus::io::endpoint{"tcp", listener->address});
    REQUIRE(channel != nullptr);

    channel->send(frame); // admitted, stalls resident (the cap now near-full)
    channel->send(frame); // over cap -> drop_newest -> shed + count

    REQUIRE(transport.dropped() > 0); // the transport folds the minted channel's shed total
    ::close(listener->fd);
}

TEST_CASE("lwip_transport forwards block to its channels: no shed, would_block surfaces", "[seam]")
{
    auto listener = make_open_listener();
    REQUIRE(listener.has_value());

    plexus::freertos::freertos_executor ex;
    const auto frame = on_wire(filler(64));
    transport_type transport(ex, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, frame.size() + 8, plexus::io::congestion::block);

    std::unique_ptr<channel_type> channel;
    transport.on_dialed([&](std::unique_ptr<channel_type> c, const plexus::io::endpoint &) { channel = std::move(c); });

    g_stall = true;
    transport.dial(plexus::io::endpoint{"tcp", listener->address});
    REQUIRE(channel != nullptr);

    int errors = 0;
    channel->on_error([&](plexus::io::io_error) { ++errors; });

    channel->send(frame); // admitted, stalls resident
    channel->send(frame); // over cap -> block -> would_block

    REQUIRE(transport.dropped() == 0); // block never sheds, so the fold stays zero
    REQUIRE(errors == 1);              // backpressure surfaced through on_error(would_block)
    ::close(listener->fd);
}
