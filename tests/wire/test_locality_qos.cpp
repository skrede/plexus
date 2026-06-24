// The locality QoS unit oracle: the composable delivery-tier bitflag enum, its
// bitwise operators, the any_set fan-out predicate, and the scheme->tier classifier
// (fail-closed: an unrecognized scheme classifies remote). A pure header-only unit —
// no backend, no transport — proving every row of the locality truth table the
// confinement gate relies on. Also proves the topic_qos.reach field defaults to
// locality::any (an undeclared topic reaches every tier — no confinement).

#include "plexus/io/locality.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using plexus::io::locality;
using plexus::io::any_set;
using plexus::io::tier_of;

namespace {

constexpr std::uint8_t bits(locality l)
{
    return static_cast<std::uint8_t>(l);
}

}

TEST_CASE("locality: each tier is a distinct power-of-two bit and any composes all three", "[wire][locality]")
{
    REQUIRE(bits(locality::process) == 1u);
    REQUIRE(bits(locality::local) == 2u);
    REQUIRE(bits(locality::remote) == 4u);
    REQUIRE(bits(locality::any) == 7u);
    REQUIRE((locality::process | locality::local | locality::remote) == locality::any);
}

TEST_CASE("locality: operator| unions the tier bits", "[wire][locality]")
{
    REQUIRE(bits(locality::process | locality::local) == 3u);
    REQUIRE(bits(locality::process | locality::remote) == 5u);
    REQUIRE(bits(locality::local | locality::remote) == 6u);
}

TEST_CASE("locality: operator& intersects the tier bits", "[wire][locality]")
{
    REQUIRE(bits(locality::any & locality::process) == bits(locality::process));
    REQUIRE(bits((locality::process | locality::local) & locality::local) == bits(locality::local));
    REQUIRE(bits(locality::process & locality::remote) == 0u);
}

TEST_CASE("locality: operator~ masks back to any's three bits only (no high bits set)", "[wire][locality]")
{
    REQUIRE((~locality::process) == (locality::local | locality::remote));
    REQUIRE((~locality::local) == (locality::process | locality::remote));
    REQUIRE((~locality::remote) == (locality::process | locality::local));
    // ~any clears every meaningful bit AND must not leave any high bit set.
    REQUIRE(bits(~locality::any) == 0u);
}

TEST_CASE("locality: any_set is true iff the mask and the tier share a bit", "[wire][locality]")
{
    REQUIRE(any_set(locality::any, locality::process));
    REQUIRE_FALSE(any_set(locality::process, locality::remote));
    REQUIRE(any_set(locality::process | locality::local, locality::local));
    REQUIRE_FALSE(any_set(locality::process | locality::local, locality::remote));
    REQUIRE_FALSE(any_set(locality::remote, locality::local));
}

TEST_CASE("locality: tier_of classifies the transport scheme, fail-closed to remote on the unknown", "[wire][locality]")
{
    REQUIRE(tier_of("inproc") == locality::process);
    REQUIRE(tier_of("unix") == locality::local);
    REQUIRE(tier_of("tcp") == locality::remote);
    REQUIRE(tier_of("tls") == locality::remote);
    REQUIRE(tier_of("udp") == locality::remote);
    REQUIRE(tier_of("bogus") == locality::remote); // fail-closed: never leak an unknown transport
    REQUIRE(tier_of("") == locality::remote);
}

TEST_CASE("locality: topic_qos.reach defaults to any so an undeclared topic reaches every tier", "[wire][locality]")
{
    plexus::topic_qos qos{};
    REQUIRE(qos.reach == locality::any);
    REQUIRE(any_set(qos.reach, locality::process));
    REQUIRE(any_set(qos.reach, locality::local));
    REQUIRE(any_set(qos.reach, locality::remote));
}

namespace {

// A do-nothing channel type to instantiate the registry template — declare/qos_for
// never touch the channel, so only the type is needed for the round-trip.
struct stub_channel
{
};

}

TEST_CASE("locality: a non-default reach survives the declare -> qos_for round-trip (the field is "
          "not truncated)",
          "[wire][locality]")
{
    plexus::io::subscriber_registry<stub_channel> registry;
    const std::uint64_t hash = 0xABCDEF01u;

    // An undeclared topic reports the default any (no confinement).
    REQUIRE(registry.qos_for(hash).reach == locality::any);

    // Declare a confined topic and read the field straight back: a non-default value
    // must round-trip, so a dropped/ignored new field is caught here.
    registry.declare(hash, "demo.topic", plexus::topic_qos{.reach = locality::local});
    REQUIRE(registry.qos_for(hash).reach == locality::local);
    REQUIRE(registry.qos_for(hash).reach != locality::any);
}

TEST_CASE("subscriber_registry: record_drop for an undeclared topic mints no entry — fqn_for stays "
          "empty (no cached empty-fqn)",
          "[wire][locality]")
{
    namespace pdetail = plexus::io::detail;
    plexus::io::subscriber_registry<stub_channel> registry;
    const std::uint64_t unknown = 0xDEADBEEFu;
    const std::size_t band      = 0;

    // A drop bumped for a never-declared topic must not create a record: it would let
    // fqn_for memoize an empty fqn as a "resolved" view, conflating unknown with unnamed.
    registry.record_drop(unknown, band, pdetail::drop_cause::drop_newest);

    REQUIRE(registry.fqn_for(unknown).empty());      // unknown stays unresolved
    REQUIRE(registry.entry_for(unknown) == nullptr); // no entry was minted
    REQUIRE(registry.dropped(unknown, band, pdetail::drop_cause::drop_newest) == 0);

    // And a real entry's fqn still resolves (the find-only path never disturbs the memo).
    const std::uint64_t known = 0x01020304u;
    registry.declare(known, "real.topic", plexus::topic_qos{});
    REQUIRE(registry.fqn_for(known) == "real.topic");
    registry.record_drop(known, band, pdetail::drop_cause::drop_newest); // a declared topic counts
    REQUIRE(registry.dropped(known, band, pdetail::drop_cause::drop_newest) == 1);
    REQUIRE(registry.fqn_for(unknown).empty()); // still unresolved after a real drop
}
