#include "test_pending_dial_registry_common.h"

using namespace pending_dial_registry_fixture;

TEST_CASE("pending_dial_registry insert_accepted+adopt_accepted transfers ownership out", "[io][pending_dial_registry]")
{
    int destroyed = 0;
    deferred_sink sink;
    registry reg{sink.callback()};

    auto ch   = std::make_unique<fake_channel>(&destroyed, 5);
    auto *raw = ch.get();
    reg.insert_accepted(raw, std::move(ch));
    REQUIRE(reg.accepted_size() == 1);

    auto adopted = reg.adopt_accepted(raw);
    REQUIRE(adopted.get() == raw);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(reg.adopt_accepted(raw) == nullptr);
}

TEST_CASE("pending_dial_registry fail_accepted routes the freed accepted channel through the "
          "deferred-destroy callback",
          "[io][pending_dial_registry]")
{
    int destroyed = 0;
    deferred_sink sink;
    registry reg{sink.callback()};

    auto ch   = std::make_unique<fake_channel>(&destroyed, 77);
    auto *raw = ch.get();
    reg.insert_accepted(raw, std::move(ch));
    REQUIRE(reg.accepted_size() == 1);

    reg.fail_accepted(raw);

    // fail_accepted() erased the accepted entry but DID NOT destroy the channel
    // synchronously — it was handed to the defer sink (the channel survives mid-stack,
    // the accept-side analog of fail()).
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(destroyed == 0);
    REQUIRE(sink.parked.size() == 1);

    // Releasing the deferred park (what the posted continuation does) destroys it.
    sink.run();
    REQUIRE(destroyed == 1);
}

TEST_CASE("pending_dial_registry fail_accepted on a miss is a no-op", "[io][pending_dial_registry]")
{
    int destroyed = 0;
    deferred_sink sink;
    registry reg{sink.callback()};

    fake_channel never_inserted{&destroyed, 0};
    reg.fail_accepted(&never_inserted);

    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(sink.parked.empty());
    REQUIRE(destroyed == 0); // nothing freed, no defer fired
}

TEST_CASE("pending_dial_registry clear empties both tables", "[io][pending_dial_registry]")
{
    int destroyed = 0;
    deferred_sink sink;
    registry reg{sink.callback()};

    auto a      = std::make_unique<fake_channel>(&destroyed, 1);
    auto b      = std::make_unique<fake_channel>(&destroyed, 2);
    auto *raw_a = a.get();
    auto *raw_b = b.get();
    reg.insert(raw_a, std::move(a));
    reg.insert_accepted(raw_b, std::move(b));
    REQUIRE(reg.pending_size() == 1);
    REQUIRE(reg.accepted_size() == 1);

    reg.clear();
    REQUIRE(reg.pending_size() == 0);
    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(destroyed == 2); // both held channels destroyed on clear
}
