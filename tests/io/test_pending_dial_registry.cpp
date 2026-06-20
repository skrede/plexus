#include "test_pending_dial_registry_common.h"

using namespace pending_dial_registry_fixture;

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
