// The saturating-publisher memory-bound proof per stream transport: a producer that
// outruns a never-reading peer drives each channel's bounded write queue to its cap and
// NO further — the byte budget is the structural memory bound, so a hostile/saturating
// publisher can never grow the userspace outbox without limit (the OOM offender the
// unbounded unix deque left possible). For tcp and unix the accepted/adopted channel end
// is fed 1 KiB frames in a tight loop while the peer never reads, and across the WHOLE
// run backpressured() stays <= cap; the loop is repeated so a leak/accumulation across
// iterations would surface as a cap breach. The same proof rides on the existing
// asio/unix bounded-outbox legs; this gate adds the LOOPED saturation framing and pins
// that both stream transports share the structural bound (the unix hand-rolled deque is
// gone). udp's bounded send queue is proven in its own suite (udp_backpressure_queue).
//
// RSS is not sampled directly: the byte cap IS the residency bound (one frame in flight
// + the shallow cap), so backpressured() <= cap over a saturating loop is the bounded-
// memory invariant. Looped in-body; the ctest invocation is re-run across process runs.

#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_channel.h"

#include "plexus/io/congestion.h"

#include "plexus/wire/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/local/stream_protocol.hpp>

#include <unistd.h>

#include <span>
#include <vector>
#include <string>
#include <cstddef>
#include <utility>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

namespace {

constexpr std::size_t k_cap    = 4096;
constexpr int         k_loops  = 4;
constexpr int         k_frames = 4096;

// Drive a never-reading-peer saturating loop over a stream channel, asserting the outbox
// never grows past the cap across the WHOLE saturating run. The channel is fed 1 KiB
// frames while its peer never reads (so async_write stalls and the userspace queue
// accumulates). A bounded channel holds at — but never past — the cap; an unbounded one
// grows without limit, breaching the assertion. The loop bound is generous enough that an
// unbounded queue blows the cap well before it ends.
template<typename Channel>
void saturate_and_assert_bounded(::asio::io_context &io, Channel &ch)
{
    std::vector<std::byte> kib(1024, std::byte{0x5A});
    for(int i = 0; i < k_frames; ++i)
    {
        ch.send(kib);
        io.poll();
        REQUIRE(ch.backpressured() <= k_cap); // the structural memory bound
    }
    REQUIRE(ch.backpressured() <= k_cap);
}

struct unix_pair
{
    std::string dir;
    std::string path;

    unix_pair()
    {
        char        tmpl[] = "/tmp/pxsat-XXXXXX";
        const char *made   = ::mkdtemp(tmpl);
        dir                = made ? made : "";
        path               = dir + "/s";
    }

    ~unix_pair()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

}

TEST_CASE("egress_saturation_bounded tcp: close() surfaces the abandoned backlog as a counted "
          "drop, never silent loss",
          "[integration][bound][saturation][close-drain]")
{
    // close() over a non-empty write-queue backlog must surface the still-unsent frames as
    // loss (dropped_count bumps by the residual frame count), never silently zero them. A
    // never-reading peer is pinned so async_write stalls and a backlog accumulates under the
    // cap; close() then abandons it (the bytes are NOT flushed — a TLS-safe synchronous
    // teardown precludes a non-blocking flush) and records the residual as a drop.
    for(int loop = 0; loop < k_loops; ++loop)
    {
        ::asio::io_context        io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   peer{io};
        ::asio::ip::tcp::socket   client{io};
        client.connect(acc.local_endpoint());
        acc.accept(peer); // peer adopts but NEVER reads

        // Floor both kernel buffers so async_write stalls after a few KiB and a backlog
        // accumulates in the userspace queue rather than draining into the kernel.
        client.set_option(::asio::socket_base::send_buffer_size{1024});
        peer.set_option(::asio::socket_base::receive_buffer_size{1024});

        // A clean close on a never-used channel reports zero residual (the common path).
        {
            ::asio::ip::tcp::socket idle_peer{io};
            ::asio::ip::tcp::socket idle_client{io};
            idle_client.connect(acc.local_endpoint());
            acc.accept(idle_peer);
            pasio::asio_channel idle{io, std::move(idle_client), wire::stream_inbound_config{},
                                     pio::congestion::block, pio::egress_capacity::of_bytes(k_cap)};
            idle.close();
            REQUIRE(idle.dropped_count() == 0); // a drained/empty close bumps nothing
            io.poll(); // drain close()'s posted on_closed while idle is still alive (it captures
                       // `this`)
        }

        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{},
                               pio::congestion::block, pio::egress_capacity::of_bytes(k_cap)};

        // Fill past one drain turn: send 1 KiB frames until the stalled queue holds a backlog
        // (backpressured() > 0 means frames are queued, undrained, behind the stalled socket).
        std::vector<std::byte> kib(1024, std::byte{0x5A});
        for(int i = 0; i < k_frames && ch.backpressured() == 0; ++i)
        {
            ch.send(kib);
            io.poll();
        }
        REQUIRE(ch.backpressured() > 0);  // a real undrained backlog is present
        REQUIRE(ch.dropped_count() == 0); // nothing shed yet (block stalls, never drops)

        ch.close();
        REQUIRE(ch.dropped_count() > 0); // the abandoned backlog surfaced as a counted drop
        io.poll(); // drain close()'s posted on_closed before ch leaves scope (the post captures
                   // `this`)
    }
}

TEST_CASE("egress_saturation_bounded tcp: a saturating publisher stays memory-bounded",
          "[integration][bound][saturation]")
{
    for(int loop = 0; loop < k_loops; ++loop)
    {
        ::asio::io_context        io;
        ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   peer{io};
        ::asio::ip::tcp::socket   client{io};
        client.connect(acc.local_endpoint());
        acc.accept(peer); // peer adopts but NEVER reads

        // The plaintext channel is already bounded — it pins the structural invariant the
        // unix leg must come to match.
        pasio::asio_channel ch{io, std::move(client), wire::stream_inbound_config{},
                               pio::congestion::block, pio::egress_capacity::of_bytes(k_cap)};
        saturate_and_assert_bounded(io, ch);
    }
}

TEST_CASE("egress_saturation_bounded unix: a saturating publisher stays memory-bounded",
          "[integration][bound][saturation][unix]")
{
    for(int loop = 0; loop < k_loops; ++loop)
    {
        unix_pair                                sock;
        ::asio::io_context                       io;
        ::asio::local::stream_protocol::acceptor acc{
                io, ::asio::local::stream_protocol::endpoint(sock.path)};
        ::asio::local::stream_protocol::socket peer{io};
        ::asio::local::stream_protocol::socket client{io};
        client.connect(::asio::local::stream_protocol::endpoint(sock.path));
        acc.accept(peer); // peer adopts but NEVER reads

        // Adopt the client end into an accept-mode unix_channel with the small cap: the
        // bounded stream_send_queue holds the outbox at the shallow cap under this
        // saturation, byte-identical to the plaintext channel above.
        pasio::unix_channel ch{io, std::move(client), wire::stream_inbound_config{},
                               pio::congestion::block, pio::egress_capacity::of_bytes(k_cap)};
        saturate_and_assert_bounded(io, ch);
    }
}
