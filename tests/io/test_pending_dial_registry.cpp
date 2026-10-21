#include "test_pending_dial_registry_common.h"

#include "plexus/io/io_error.h"

#include <string>
#include <cstdint>

using namespace pending_dial_registry_fixture;

TEST_CASE("pending_dial_registry insert+resolve returns the same owned channel and erases the entry", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch));
    REQUIRE(reg.pending_size() == 1);

    auto resolved = reg.resolve(raw);
    REQUIRE(resolved.get() == raw);
    REQUIRE(reg.pending_size() == 0);
    REQUIRE(reg.resolve(raw) == nullptr);
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(executor.queued() == 0);
}

TEST_CASE("pending_dial_registry resolve honors copy-before-erase (a copied-out value survives the erase)", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    plexus::io::pending_dial_registry<contract_channel, std::string> reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), std::string{"203.0.113.5:9000"});

    // Under asan a read of the entry after the erase would corrupt this copy.
    const std::string endpoint_copy = *reg.payload_of(raw);
    auto resolved                   = reg.resolve(raw);

    REQUIRE(resolved.get() == raw);
    REQUIRE(endpoint_copy == "203.0.113.5:9000");
    REQUIRE(reg.payload_of(raw) == nullptr);
}

TEST_CASE("pending_dial_registry fail keeps a dial with an operation pending until its own close notification", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    ch->start_operation();
    reg.insert(raw, std::move(ch));

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
    reg.insert(raw, std::move(ch));
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

TEST_CASE("pending_dial_registry threads a non-monostate Payload through payload_of", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    plexus::io::pending_dial_registry<contract_channel, std::int32_t> reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), 1234);

    REQUIRE(reg.payload_of(raw) != nullptr);
    REQUIRE(*reg.payload_of(raw) == 1234);
    *reg.payload_of(raw) = 5678;
    REQUIRE(*reg.payload_of(raw) == 5678);
    REQUIRE(reg.payload_of(nullptr) == nullptr);
}
