#include "contract_channel.h"

#include "support/retire_drain.h"

#include "plexus/io/io_error.h"

#include "plexus/io/detail/channel_graveyard.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

using plexus::testing::fifo_executor;
using plexus::testing::contract_channel;
using event_log = std::vector<std::string>;

template<std::size_t N>
using graveyard = plexus::io::detail::channel_graveyard<contract_channel, N>;

namespace {

std::unique_ptr<contract_channel> make_channel(fifo_executor &executor, event_log &log)
{
    return std::make_unique<contract_channel>(executor, log);
}

}

TEST_CASE("channel_graveyard a channel retired with an operation pending is destroyed only after that operation and its close notification ran", "[io][channel_graveyard]")
{
    fifo_executor executor;
    event_log log;
    graveyard<1> yard;
    auto channel = make_channel(executor, log);
    channel->start_operation();
    const contract_channel &retired = *channel;

    yard.retire(std::move(channel), [&log] { log.push_back("erased"); });
    REQUIRE(yard.holds(retired));
    REQUIRE(log.empty());

    executor.drain();
    REQUIRE(yard.empty());
    REQUIRE(log == event_log{"operation", "closed", "destroyed", "erased"});
}

TEST_CASE("channel_graveyard a channel that failed and quiesced before it was retired is erased after one posted hop", "[io][channel_graveyard]")
{
    fifo_executor executor;
    event_log log;
    graveyard<1> yard;
    auto channel = make_channel(executor, log);
    channel->on_error([&log](plexus::io::io_error) { log.push_back("error"); });
    channel->fail(plexus::io::io_error::connection_reset);
    REQUIRE(executor.queued() == 0);

    yard.retire(std::move(channel), [&log] { log.push_back("erased"); });
    REQUIRE(executor.queued() == 1);
    REQUIRE(executor.run_one());
    REQUIRE(yard.empty());
    REQUIRE(executor.queued() == 0);
    REQUIRE(log == event_log{"error", "closed", "destroyed", "erased"});
}

TEST_CASE("channel_graveyard a retire into a full graveyard fails closed", "[io][channel_graveyard]")
{
    fifo_executor executor;
    event_log log;
    graveyard<1> yard;
    auto first = make_channel(executor, log);
    first->start_operation();
    yard.retire(std::move(first));

    REQUIRE_THROWS_AS(yard.retire(make_channel(executor, log)), std::runtime_error);
    REQUIRE(yard.size() == 1);
    executor.drain();
    REQUIRE(yard.empty());
}

TEST_CASE("channel_graveyard retiring a null channel is a no-op", "[io][channel_graveyard]")
{
    graveyard<1> yard;
    bool erased = false;
    yard.retire(nullptr, [&erased] { erased = true; });
    REQUIRE(yard.empty());
    REQUIRE_FALSE(erased);
}

TEST_CASE("channel_graveyard cells erase independently and in completion order", "[io][channel_graveyard]")
{
    fifo_executor executor;
    event_log log;
    graveyard<2> yard;
    auto slow = make_channel(executor, log);
    auto fast = make_channel(executor, log);
    slow->start_operation();
    const contract_channel &slow_ref = *slow;

    yard.retire(std::move(slow), [&log] { log.push_back("erased slow"); });
    yard.retire(std::move(fast), [&log] { log.push_back("erased fast"); });
    REQUIRE(yard.size() == 2);
    REQUIRE(yard.holds(slow_ref));

    REQUIRE(executor.run_one());
    REQUIRE(yard.size() == 2);
    REQUIRE(executor.run_one());
    REQUIRE(yard.size() == 1);
    REQUIRE(yard.holds(slow_ref));
    REQUIRE(executor.run_one());
    REQUIRE(yard.empty());
    REQUIRE(log == event_log{"operation", "closed", "destroyed", "erased fast", "closed", "destroyed", "erased slow"});
}

TEST_CASE("channel_graveyard retire_and_drain reports one erase after every queued handler of the channel ran", "[io][channel_graveyard]")
{
    fifo_executor executor;
    event_log log;
    auto channel = make_channel(executor, log);
    channel->start_operation();

    auto outcome = plexus::testing::retire_and_drain(channel,
                                                     [&executor, &log]
                                                     {
                                                         executor.drain();
                                                         log.push_back("drained");
                                                     });
    REQUIRE(channel == nullptr);
    REQUIRE(outcome.erased == 1);
    REQUIRE(outcome.empty);
    REQUIRE(log == event_log{"operation", "closed", "destroyed", "drained"});
}
