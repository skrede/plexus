#include "drain_after_destroy_common.h"
#include "stream_retire_drain_common.h"

#include "support/alloc_watch.h"
#include "support/retire_drain.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace pasio  = plexus::asio;
namespace pio    = plexus::io;
namespace stream = plexus::stream;

using namespace drain_after_destroy;
using namespace stream_retire_drain;
using plexus::testing::retire_and_drain;

namespace {

constexpr std::size_t k_read_bytes     = 12289;
constexpr std::size_t k_inflight_bytes = 262147;

std::unique_ptr<pasio::asio_channel> stalled_channel(tcp_pair &h, std::size_t read_bytes = pasio::k_stream_read_buffer_bytes)
{
    return std::make_unique<pasio::asio_channel>(h.io, std::move(h.client), stream::stream_inbound_config{}, pio::congestion::block, pio::egress_capacity::of_bytes(1u << 20u),
                                                 pasio::stream_socket_options{}, read_bytes);
}

void stall_one_frame(tcp_pair &h, pasio::asio_channel &ch)
{
    const std::vector<std::byte> frame(k_inflight_bytes, std::byte{0x5A});
    plexus::testing::watch_next_alloc(k_inflight_bytes);
    ch.send(frame);
    REQUIRE(plexus::testing::watched_allocated());
    h.io.poll();
    REQUIRE(ch.outstanding_ops() > 1);
}

// Retires the channel and reports whether the watched block was already freed when the drain began.
bool freed_before_the_drain(std::unique_ptr<pasio::asio_channel> &ch, tcp_pair &h)
{
    bool freed_at_close = true;
    const auto outcome  = retire_and_drain(ch,
                                           [&]
                                           {
                                              freed_at_close = plexus::testing::watched_freed();
                                              drain(h.io);
                                           });
    REQUIRE(outcome.erased == 1);
    return freed_at_close;
}

}

TEST_CASE("stream_retire_drain a live close keeps a pending gather's frames until the aborted write drained", "[integration][asio][teardown][!shouldfail]")
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        tcp_pair h;
        auto ch = stalled_channel(h);
        stall_one_frame(h, *ch);
        REQUIRE_FALSE(freed_before_the_drain(ch, h));
        REQUIRE(plexus::testing::watched_freed());
    }
}

TEST_CASE("stream_retire_drain the read buffer is freed only after the aborted read drained", "[integration][asio][teardown]")
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        tcp_pair h;
        plexus::testing::watch_next_alloc(k_read_bytes);
        auto ch = stalled_channel(h, k_read_bytes);
        REQUIRE(plexus::testing::watched_allocated());
        h.io.poll();
        REQUIRE_FALSE(freed_before_the_drain(ch, h));
        REQUIRE(plexus::testing::watched_freed());
    }
}

// The peer half-closes while the write is stalled, so the read fails and the fail path closes the egress.
TEST_CASE("stream_retire_drain an in-flight frame's owner is freed only after the aborted write drained", "[integration][asio][teardown][!shouldfail]")
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        tcp_pair h;
        auto ch      = stalled_channel(h);
        bool errored = false;
        ch->on_error([&](pio::io_error) { errored = true; });
        stall_one_frame(h, *ch);
        h.peer.shutdown(::asio::ip::tcp::socket::shutdown_send);
        REQUIRE(poll_one_until(h.io, [&] { return errored; }));
        REQUIRE_FALSE(plexus::testing::watched_freed());
        REQUIRE_FALSE(freed_before_the_drain(ch, h));
        REQUIRE(plexus::testing::watched_freed());
    }
}
