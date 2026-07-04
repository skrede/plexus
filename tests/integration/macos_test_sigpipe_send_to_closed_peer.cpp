// A send to a peer that has closed its read side is the classic SIGPIPE-on-write process kill.
// The real plexus transports are asio sockets, and asio sets SO_NOSIGPIPE per-fd at socket-open on
// Apple, so the production send paths are already signal-safe — this file is the PROOF of that,
// not new production code. Each proof opens a real plexus stream channel for egress WITHOUT a read
// loop (mark_open, no start_read), so the ONLY path that can surface an error is the write
// completion — isolating the send-to-closed-peer syscall the SIGPIPE risk lives on. Reaching the
// assertion at all proves the process was not killed; the surfaced io_error proves the failed send
// became a hard drop routed to on_error rather than a silent hang. The whole file is macOS-gated in
// CMake (if(APPLE)); the peer's abortive close forces a reset the next write lands on.

#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_channel.h"

#include "plexus/io/io_error.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>
#include <asio/socket_base.hpp>
#include <asio/local/stream_protocol.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <filesystem>
#include <system_error>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

// Large enough that a single async_write cannot be fully absorbed by the kernel send buffer, so
// the write must actually push bytes at the closed peer and see the reset — a handful of tiny
// writes would sit in the send buffer and complete before the peer's RST is ever detected.
constexpr std::size_t k_send_bytes = 4u * 1024u * 1024u;

std::vector<std::byte> payload_bytes()
{
    std::vector<std::byte> v(k_send_bytes);
    for(std::size_t i = 0; i < v.size(); ++i)
        v[i] = static_cast<std::byte>(i);
    return v;
}

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!pred() && std::chrono::steady_clock::now() < deadline)
        io.poll();
}

bool is_hard_drop(pio::io_error e)
{
    return e == pio::io_error::broken_pipe || e == pio::io_error::connection_reset;
}

// Stand up a real plexus stream connection over the given bind endpoint, abortively close the peer,
// then issue several sends into the now-closed peer, returning the hard error the channel surfaced
// (nullopt if none did). The channel is opened for egress alone (mark_open, no read loop).
template<typename Protocol, typename Channel>
std::optional<pio::io_error> send_to_closed_peer(const typename Protocol::endpoint &bind_ep)
{
    ::asio::io_context io;

    typename Protocol::acceptor acceptor(io);
    acceptor.open(bind_ep.protocol());
    acceptor.bind(bind_ep);
    acceptor.listen();

    Channel channel(io);
    channel.socket().connect(acceptor.local_endpoint());

    typename Protocol::socket peer(io);
    acceptor.accept(peer);

    channel.mark_open();

    std::optional<pio::io_error> surfaced;
    channel.on_error([&surfaced](pio::io_error e) { surfaced = e; });

    // Abortive close so the next write lands on a reset connection deterministically (a TCP RST
    // rather than a lingering FIN); the linger option is harmless where it does not apply.
    std::error_code ec;
    (void)peer.set_option(::asio::socket_base::linger(true, 0), ec);
    (void)peer.close(ec);

    const auto payload = payload_bytes();
    for(int i = 0; i < 8 && !surfaced; ++i)
    {
        channel.send(payload);
        pump_until(io, [&surfaced] { return surfaced.has_value(); });
    }
    return surfaced;
}

}

TEST_CASE("sigpipe closed_peer: a send over a real plexus TCP-loopback path does not kill the process", "[integration][sigpipe][closed_peer]")
{
    // A signal-safety claim is never made from one run: loop the whole proof in-body, and the
    // ctest invocation is additionally re-run across >=3 process runs.
    for(int run = 0; run < 3; ++run)
    {
        const ::asio::ip::tcp::endpoint bind_ep(::asio::ip::make_address("127.0.0.1"), 0);
        const auto err = send_to_closed_peer<::asio::ip::tcp, pasio::asio_channel>(bind_ep);
        REQUIRE(err.has_value());
        REQUIRE(is_hard_drop(*err));
    }
}

TEST_CASE("sigpipe closed_peer: a send over a real plexus AF_UNIX path does not kill the process", "[integration][sigpipe][closed_peer]")
{
    for(int run = 0; run < 3; ++run)
    {
        const auto dir  = plexus::testing::make_temp_dir("pxs-").string();
        const auto path = dir + "/s";
        {
            const ::asio::local::stream_protocol::endpoint bind_ep(path);
            const auto err = send_to_closed_peer<::asio::local::stream_protocol, pasio::unix_channel>(bind_ep);
            REQUIRE(err.has_value());
            REQUIRE(is_hard_drop(*err));
        }
        plexus::testing::remove_socket_path(path);
        std::filesystem::remove(dir);
    }
}
