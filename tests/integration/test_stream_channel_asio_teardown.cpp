#include "test_stream_channel_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace stream_channel_asio_fixture;

namespace {

// A connected loopback pair whose peer end NEVER reads and whose kernel buffers are floored, so a
// few KiB stalls async_write outright (the backpressure idiom from the sibling stall test). The
// channel adopts the client end in accept-mode (reading immediately).
struct stalled_pair
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
    ::asio::ip::tcp::socket peer{io};
    ::asio::ip::tcp::socket client{io};

    stalled_pair()
    {
        client.connect(acc.local_endpoint());
        acc.accept(peer);
        client.set_option(::asio::socket_base::send_buffer_size{1024});
        peer.set_option(::asio::socket_base::receive_buffer_size{1024});
    }
};

}

TEST_CASE("asio stream channel: destroying a channel after a read-EOF fail with a stalled write in "
          "flight is teardown-clean and the fail closed the socket",
          "[integration][asio][hardening][teardown]")
{
    // The reachable teardown UAF, end to end over a real socket: a stalled write is in flight
    // when the peer half-closes, the read hits EOF and stream_fail runs, and the channel is then
    // destroyed (the redial-replacement) with the aborted write completion still queued. Pre-fix
    // the send_queue completion reads m_open on the freed m_egress (an ASan heap-use-after-free);
    // post-fix stream_fail closes the socket AND the completion sees the channel is gone and
    // no-ops. Looped for reproducibility under asan.
    constexpr int k_iterations = 8;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stalled_pair h;

        constexpr std::size_t cap = 1u * 1024u * 1024u;
        auto ch                   = std::make_unique<pasio::asio_channel>(h.io, std::move(h.client), stream::stream_inbound_config{}, pio::congestion::block,
                                                                          pio::egress_capacity::of_bytes(cap));

        bool errored = false;
        ch->on_error([&](pio::io_error) { errored = true; });

        // Stall a write: more bytes than the floored kernel buffers can absorb, so async_write
        // stays pending (the peer never drains it).
        std::vector<std::byte> big(256u * 1024u, std::byte{0x5A});
        ch->send(big);
        h.io.poll();                            // issue the async_write; it cannot complete
        REQUIRE(ch->outstanding_ops() > 0);     // a read and the stalled write are in flight

        // Peer half-closes its send side: the channel reads EOF -> stream_fail. The write stays
        // stalled (the peer receive side is still open but never drained).
        h.peer.shutdown(::asio::ip::tcp::socket::shutdown_send);

        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!errored && std::chrono::steady_clock::now() < bound)
            h.io.poll_one();
        REQUIRE(errored);
        REQUIRE(!ch->socket().is_open()); // stream_fail closed the socket; the stalled write cannot survive

        // Redial replacement: destroy the old channel with the aborted write completion still
        // queued, then poll. The completion (and any posted on_closed) must no-op, not read freed
        // storage.
        ch.reset();
        h.io.poll();
        h.io.run_for(std::chrono::milliseconds(50));
        SUCCEED("teardown after an in-flight-write stream_fail did not touch freed memory");
    }
}

TEST_CASE("asio stream channel: on_closed is posted (never inline) and fires exactly once on the "
          "close() path",
          "[integration][asio][hardening][teardown]")
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
    ::asio::ip::tcp::socket peer{io};
    ::asio::ip::tcp::socket client{io};
    client.connect(acc.local_endpoint());
    acc.accept(peer);

    pasio::asio_channel ch{io, std::move(client), stream::stream_inbound_config{}, pio::congestion::block};

    int closed          = 0;
    bool later_turn     = false;
    bool observed_later = false;
    ch.on_closed(
            [&]
            {
                ++closed;
                observed_later = later_turn;
            });

    ch.close();        // posts on_closed
    later_turn = true; // flipped AFTER close() returns, BEFORE the posted handler runs
    io.poll();         // the posted on_closed runs here

    REQUIRE(closed == 1);    // exactly once
    REQUIRE(observed_later); // ran on a later turn -> posted, not fired inline from close()

    ch.close(); // a second close must not re-fire (socket closed + the fire-once latch)
    io.poll();
    REQUIRE(closed == 1);
}

TEST_CASE("asio stream channel: on_closed is posted (never inline) and fires exactly once on the "
          "stream_fail path",
          "[integration][asio][hardening][teardown]")
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
    ::asio::ip::tcp::socket peer{io};
    ::asio::ip::tcp::socket client{io};
    client.connect(acc.local_endpoint());
    acc.accept(peer);

    pasio::asio_channel ch{io, std::move(client), stream::stream_inbound_config{}, pio::congestion::block};

    int closed   = 0;
    bool errored = false;
    ch.on_error([&](pio::io_error) { errored = true; });
    ch.on_closed([&] { ++closed; });

    peer.close(); // the channel reads EOF -> stream_fail (on_error inline, on_closed posted)

    // Drive one handler at a time: the single ready handler is the read completion, which runs
    // stream_fail synchronously (setting errored and POSTING on_closed). Were on_closed fired
    // inline, closed would already be 1 the instant that completion returns; posted means it is
    // still 0 until the next turn.
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!errored && std::chrono::steady_clock::now() < bound)
        io.poll_one();
    REQUIRE(errored);
    REQUIRE(closed == 0); // posted: not delivered inline from within the completion handler

    io.poll();
    REQUIRE(closed == 1); // fires exactly once on a later turn
}

TEST_CASE("asio stream channel: destroying the channel between a stream_fail and the next poll is "
          "teardown-clean",
          "[integration][asio][hardening][teardown]")
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc{io, ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
    ::asio::ip::tcp::socket peer{io};
    ::asio::ip::tcp::socket client{io};
    client.connect(acc.local_endpoint());
    acc.accept(peer);

    auto ch      = std::make_unique<pasio::asio_channel>(io, std::move(client), stream::stream_inbound_config{}, pio::congestion::block);
    bool errored = false;
    ch->on_error([&](pio::io_error) { errored = true; });
    ch->on_closed([&] { FAIL("on_closed must not fire after the owner is destroyed"); });

    peer.close();
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!errored && std::chrono::steady_clock::now() < bound)
        io.poll_one();
    REQUIRE(errored);

    // Destroy the channel while the posted on_closed is still queued: the closure captures the
    // shared liveness block, sees the channel is gone, and no-ops rather than dangling on this.
    ch.reset();
    io.poll();
    io.run_for(std::chrono::milliseconds(50));
    SUCCEED("the posted on_closed no-op'd after owner destruction");
}
