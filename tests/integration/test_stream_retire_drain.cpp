#include "drain_after_destroy_common.h"
#include "stream_retire_drain_common.h"

#include "support/retire_drain.h"

#include "asio/queued_teardown.h"

#include "plexus/wire/close_cause.h"

#include "plexus/io/detail/channel_graveyard.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/write.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace pasio  = plexus::asio;
namespace pio    = plexus::io;
namespace stream = plexus::stream;

using namespace std::chrono_literals;
using namespace drain_after_destroy;
using namespace stream_retire_drain;
using plexus::testing::retire_and_drain;

namespace {

// Running the first of the two posted deliveries leaves the second queued when the channel is retired.
template<typename Pair>
void retire_with_a_delivery_queued()
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        Pair h;
        auto ch                = std::make_unique<typename Pair::channel>(h.io, std::move(h.client));
        std::int32_t delivered = 0;
        ch->on_data([&](std::span<const std::byte>) { ++delivered; });
        const auto bytes = two_frames();
        ::asio::write(h.peer, ::asio::buffer(bytes));
        REQUIRE(poll_one_until(h.io, [&] { return delivered > 0; }));
        const auto outcome = retire_and_drain(ch, [&] { drain(h.io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(delivered == 1);
    }
}

// The fail's own close notification runs before the retire, with no handler installed yet.
template<typename Pair>
void retire_after_a_fail()
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        Pair h;
        auto ch      = std::make_unique<typename Pair::channel>(h.io, std::move(h.client));
        bool errored = false;
        ch->on_error([&](pio::io_error) { errored = true; });
        h.peer.close();
        REQUIRE(poll_one_until(h.io, [&] { return errored; }));
        h.io.poll();
        const auto outcome = retire_and_drain(ch, [&] { drain(h.io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(outcome.empty);
    }
}

}

TEST_CASE("stream_retire_drain a frame delivery queued before the channel closed is not handed to the consumer (tcp)", "[integration][asio][teardown]")
{
    retire_with_a_delivery_queued<tcp_pair>();
}

#if !defined(_WIN32)
TEST_CASE("stream_retire_drain a frame delivery queued before the channel closed is not handed to the consumer (unix)", "[integration][asio][teardown]")
{
    retire_with_a_delivery_queued<unix_pair>();
}
#endif

TEST_CASE("stream_retire_drain a channel that failed before it was retired is still erased (tcp)", "[integration][asio][teardown]")
{
    retire_after_a_fail<tcp_pair>();
}

#if !defined(_WIN32)
TEST_CASE("stream_retire_drain a channel that failed before it was retired is still erased (unix)", "[integration][asio][teardown]")
{
    retire_after_a_fail<unix_pair>();
}
#endif

// The retire runs in the reactor pass that already queued the expiry, so the expiry reaches a closed channel.
TEST_CASE("stream_retire_drain a no-progress expiry queued in the closing pass does nothing to the closed channel", "[integration][asio][teardown]")
{
    const auto partial = partial_frame();
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        tcp_pair h;
        const stream::stream_inbound_config cfg{.no_progress_floor = 40ms, .min_throughput_bytes_per_sec = 0};
        auto ch              = std::make_unique<pasio::asio_channel>(h.io, std::move(h.client), cfg);
        bool protocol_closed = false;
        ch->on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });
        ::asio::write(h.peer, ::asio::buffer(partial));
        h.io.run_for(5ms);

        std::int32_t erased = 0;
        pio::detail::channel_graveyard<pasio::asio_channel, 1> graveyard;
        plexus::testing::queued_action closing{h.io, [&] { graveyard.retire(std::move(ch), [&erased] { ++erased; }); }};
        closing.run_after(80ms);

        REQUIRE(erased == 1);
        REQUIRE(graveyard.empty());
        REQUIRE_FALSE(protocol_closed);
    }
}

// One complete frame and a partial one in a single read: the delivery is posted, the read re-armed and the no-progress
// wait armed when the channel is retired. The erase must be the last handler the drain runs.
TEST_CASE("stream_retire_drain a retired channel's posts, read and no-progress timer all complete before its on_closed", "[integration][asio][teardown]")
{
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        tcp_pair h;
        auto ch                = std::make_unique<pasio::asio_channel>(h.io, std::move(h.client));
        std::int32_t delivered = 0;
        ch->on_data([&](std::span<const std::byte>) { ++delivered; });
        auto bytes         = two_frames();
        const auto partial = partial_frame();
        bytes.resize(bytes.size() / 2);
        bytes.insert(bytes.end(), partial.begin(), partial.end());
        ::asio::write(h.peer, ::asio::buffer(bytes));
        REQUIRE(poll_one_until(h.io, [&] { return ch->outstanding_ops() == 2; }));

        std::int32_t ran          = 0;
        std::int32_t ran_at_erase = -1;
        const auto drain_counting = [&]
        {
            while(h.io.poll_one() != 0)
                ++ran;
            while(h.io.run_one_for(20ms) != 0)
                ++ran;
        };
        const auto outcome = retire_and_drain(ch, drain_counting, [&] { ran_at_erase = ran; });

        REQUIRE(outcome.erased == 1);
        REQUIRE(outcome.empty);
        REQUIRE(delivered == 0);
        REQUIRE(ran_at_erase >= 3);
        REQUIRE(ran == ran_at_erase + 1);
    }
}
