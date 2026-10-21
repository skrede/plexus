#include "test_pending_dial_registry_common.h"

using namespace pending_dial_registry_fixture;

TEST_CASE("pending_dial_registry insert_accepted+adopt_accepted transfers ownership out", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    reg.insert_accepted(raw, std::move(ch));
    REQUIRE(reg.accepted_size() == 1);

    auto adopted = reg.adopt_accepted(raw);
    REQUIRE(adopted.get() == raw);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(reg.adopt_accepted(raw) == nullptr);
    REQUIRE(executor.queued() == 0);
}

TEST_CASE("pending_dial_registry fail_accepted keeps the accepted channel until its own close notification", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto ch   = make_channel(executor, log);
    auto *raw = ch.get();
    ch->start_operation();
    reg.insert_accepted(raw, std::move(ch));

    reg.fail_accepted(raw);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(reg.retiring_size() == 1);
    REQUIRE(log.empty());

    executor.drain();
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(log == event_log{"operation", "closed", "destroyed"});
}

TEST_CASE("pending_dial_registry fail_accepted on a miss is a no-op", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    contract_channel never_inserted{executor, log};

    reg.fail_accepted(&never_inserted);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(executor.queued() == 0);
}

TEST_CASE("pending_dial_registry retire_all retires both tables and destroys each channel only inside its own close notification", "[io][pending_dial_registry]")
{
    fifo_executor executor;
    event_log log;
    registry reg;
    auto dialed    = make_channel(executor, log);
    auto accepted  = make_channel(executor, log);
    auto *raw_dial = dialed.get();
    auto *raw_acc  = accepted.get();
    dialed->start_operation();
    reg.insert(raw_dial, std::move(dialed));
    reg.insert_accepted(raw_acc, std::move(accepted));

    reg.retire_all();
    REQUIRE(reg.pending_size() == 0);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(reg.retiring_size() == 2);
    REQUIRE_FALSE(reg.is_pending(raw_dial));
    REQUIRE(reg.resolve(raw_dial) == nullptr);
    REQUIRE(reg.adopt_accepted(raw_acc) == nullptr);
    REQUIRE(log.empty());

    executor.drain();
    REQUIRE(reg.retiring_size() == 0);
    REQUIRE(log == event_log{"operation", "closed", "destroyed", "closed", "destroyed"});
}
