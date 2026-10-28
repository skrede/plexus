#include "test_pending_dial_registry_common.h"

#include "plexus/io/io_error.h"

using namespace pending_dial_registry_fixture;

TEST_CASE("pending_dial_registry insert+resolve returns the same owned channel and erases the entry", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), dial_target());
    REQUIRE(reg.pending_size() == 1);
    REQUIRE(reg.is_pending(raw));

    auto resolved = reg.resolve(raw);
    REQUIRE(resolved.get() == raw);
    REQUIRE(reg.pending_size() == 0);
    REQUIRE_FALSE(reg.is_pending(raw));
    REQUIRE(reg.resolve(raw) == nullptr);
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(executor.queued() == 0);
}

TEST_CASE("pending_dial_registry resolve erases only the entry and neither closes nor destroys the channel it hands out", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), dial_target());

    auto resolved = reg.resolve(raw);
    executor.drain();

    REQUIRE(resolved.get() == raw);
    REQUIRE(log.empty());
}

TEST_CASE("pending_dial_registry fail keeps a dial with an operation pending until its own close notification", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    ch->start_operation();
    reg.insert(raw, std::move(ch), dial_target());

    reg.fail(raw);
    REQUIRE(reg.pending_size() == 0);
    REQUIRE(reg.retiring_size() == 1);
    REQUIRE(log.empty());

    executor.drain();
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(log == event_log{"operation", "closed", "destroyed"});
}

TEST_CASE("pending_dial_registry fail of a dial already closed and quiet erases it after one posted hop", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    ch->fail(plexus::io::io_error::connection_reset);
    reg.insert(raw, std::move(ch), dial_target());
    REQUIRE(executor.queued() == 0);

    reg.fail(raw);
    REQUIRE(reg.retiring_size() == 1);
    REQUIRE(executor.queued() == 1);
    REQUIRE(executor.run_one());
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(executor.queued() == 0);
    REQUIRE(log == event_log{"closed", "destroyed"});
}

TEST_CASE("pending_dial_registry fail on a miss is a no-op", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    contract_channel never_inserted{executor, log};

    reg.fail(&never_inserted);
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(executor.queued() == 0);
}

TEST_CASE("pending_dial_registry retire_all never reports a dial already resolved or failed", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    dial_failed_sink sink = log_reports(log);
    registry reg;
    auto first       = make_channel(executor, log);
    auto second      = make_channel(executor, log);
    auto third       = make_channel(executor, log);
    auto *raw_first  = first.get();
    auto *raw_second = second.get();
    auto *raw_third  = third.get();
    reg.insert(raw_first, std::move(first), {"tcp", "127.0.0.1:7"});
    reg.insert(raw_second, std::move(second), {"tcp", "127.0.0.1:8"});
    reg.insert(raw_third, std::move(third), {"tcp", "127.0.0.1:9"});

    auto resolved = reg.resolve(raw_first);
    reg.fail(raw_second);
    reg.retire_all(sink);
    executor.drain();

    REQUIRE(resolved.get() == raw_first);
    REQUIRE(log == event_log{"closed", "destroyed", "closed", "destroyed", "aborted 127.0.0.1:9"});
}
