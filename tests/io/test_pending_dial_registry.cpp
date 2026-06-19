// The pending_dial_registry oracle: a pure sans-IO drive of the generic half-open
// dial table + accepted table + ownership-transfer, with a destruction-counting
// stand-in Channel (a recording POD, no socket and no backend link — plexus::plexus
// only). It proves the lifetime contracts the asio transports relied on: insert+resolve
// returns the SAME owned channel and erases the entry; the copy-before-erase contract
// (a value copied out before resolve stays intact across the erase); fail() routes the
// freed channel through the INJECTED defer-destroy callback — destroyed deferred, NOT
// synchronously inside fail(); insert_accepted/adopt_accepted transfers ownership out;
// clear() empties both tables; and a non-monostate Payload round-trips via payload_of.

#include "plexus/io/pending_dial_registry.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace {

// A recording stand-in channel: bumps a shared counter on destruction so the test can
// observe exactly WHEN (synchronously vs. deferred) a freed channel is torn down.
struct fake_channel
{
    int *destroyed;
    int  id;

    explicit fake_channel(int *counter, int identity)
            : destroyed(counter)
            , id(identity)
    {
    }

    ~fake_channel() { ++*destroyed; }

    fake_channel(const fake_channel &)            = delete;
    fake_channel &operator=(const fake_channel &) = delete;
};

using registry = plexus::io::pending_dial_registry<fake_channel>;

// A defer-destroy sink that PARKS the freed channel instead of destroying it, so the
// test can assert the channel survives fail() and is torn down only when the park is
// released (the deferred edge — what a posted continuation does on the real path).
struct deferred_sink
{
    std::vector<std::unique_ptr<fake_channel>> parked;

    registry::defer_destroy callback()
    {
        return [this](std::unique_ptr<fake_channel> ch) { parked.push_back(std::move(ch)); };
    }

    void run() { parked.clear(); }
};

}

TEST_CASE(
        "pending_dial_registry insert+resolve returns the same owned channel and erases the entry",
        "[io][pending_dial_registry]")
{
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 42);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch));
    REQUIRE(reg.pending_size() == 1);

    auto resolved = reg.resolve(raw);
    REQUIRE(resolved.get() == raw); // the SAME owned channel handed back
    REQUIRE(resolved->id == 42);
    REQUIRE(reg.pending_size() == 0); // the entry was erased

    REQUIRE(reg.resolve(raw) == nullptr); // a second resolve misses
}

TEST_CASE("pending_dial_registry resolve honors copy-before-erase (a copied-out value survives the "
          "erase)",
          "[io][pending_dial_registry]")
{
    int                                                          destroyed = 0;
    deferred_sink                                                sink;
    plexus::io::pending_dial_registry<fake_channel, std::string> reg{
            [](std::unique_ptr<fake_channel>) {}};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 7);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), std::string{"203.0.113.5:9000"});

    // Copy the entry-bound value OUT before resolve erases the entry; under asan a
    // post-erase read of the entry would corrupt this copy.
    const std::string endpoint_copy = *reg.payload_of(raw);
    auto              resolved      = reg.resolve(raw);

    REQUIRE(resolved.get() == raw);
    REQUIRE(endpoint_copy == "203.0.113.5:9000");
    REQUIRE(reg.payload_of(raw) == nullptr); // the entry (and its payload) are gone
}

TEST_CASE(
        "pending_dial_registry fail routes the freed channel through the deferred-destroy callback",
        "[io][pending_dial_registry]")
{
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 99);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch));

    reg.fail(raw);

    // fail() erased the entry but DID NOT destroy the channel synchronously — it was
    // handed to the defer sink, which parks it (the channel survives mid-stack).
    REQUIRE(reg.pending_size() == 0);
    REQUIRE(destroyed == 0);
    REQUIRE(sink.parked.size() == 1);

    // Releasing the deferred park (what the posted continuation does) destroys it.
    sink.run();
    REQUIRE(destroyed == 1);
}

TEST_CASE("pending_dial_registry insert_accepted+adopt_accepted transfers ownership out",
          "[io][pending_dial_registry]")
{
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 5);
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
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 77);
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
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    fake_channel never_inserted{&destroyed, 0};
    reg.fail_accepted(&never_inserted);

    REQUIRE(reg.accepted_size() == 0);
    REQUIRE(sink.parked.empty());
    REQUIRE(destroyed == 0); // nothing freed, no defer fired
}

TEST_CASE("pending_dial_registry clear empties both tables", "[io][pending_dial_registry]")
{
    int           destroyed = 0;
    deferred_sink sink;
    registry      reg{sink.callback()};

    auto  a     = std::make_unique<fake_channel>(&destroyed, 1);
    auto  b     = std::make_unique<fake_channel>(&destroyed, 2);
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

TEST_CASE("pending_dial_registry threads a non-monostate Payload through payload_of",
          "[io][pending_dial_registry]")
{
    int                                                  destroyed = 0;
    plexus::io::pending_dial_registry<fake_channel, int> reg{[](std::unique_ptr<fake_channel>) {}};

    auto  ch  = std::make_unique<fake_channel>(&destroyed, 3);
    auto *raw = ch.get();
    reg.insert(raw, std::move(ch), 1234);

    REQUIRE(reg.payload_of(raw) != nullptr);
    REQUIRE(*reg.payload_of(raw) == 1234);

    // The payload is mutable in place (the ARQ a UDP entry drives lives here).
    *reg.payload_of(raw) = 5678;
    REQUIRE(*reg.payload_of(raw) == 5678);

    REQUIRE(reg.payload_of(nullptr) == nullptr);
}
