// The fused-liveliness value-type oracle: pins the liveliness_signal bitmask algebra, the
// liveliness_verdict spellings, the liveliness_options default field values, and that the two
// new observer virtuals default to no-op/unobserved yet carry a hand-built verdict unchanged
// once overridden. Header-only core, linked against plexus::core + Catch2's main only.

#include "plexus/io/observer.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using plexus::io::combine;
using plexus::io::liveliness_options;
using plexus::io::liveliness_signal;
using plexus::io::liveliness_verdict;
using plexus::io::observer;
using plexus::io::peer_liveliness_event;
using plexus::node_id;

namespace {

class capturing_observer : public observer
{
public:
    bool observes_liveliness() const override
    {
        return true;
    }

    void on_peer_liveliness(const peer_liveliness_event &ev) override
    {
        last = ev;
    }

    std::optional<peer_liveliness_event> last;
};

} // namespace

TEST_CASE("peer_liveliness types bitmask composes and tests single bits", "[io][peer_liveliness]")
{
    REQUIRE(static_cast<std::uint8_t>(liveliness_signal::none) == 0);
    REQUIRE(static_cast<std::uint8_t>(liveliness_signal::awareness) == 1);
    REQUIRE(static_cast<std::uint8_t>(liveliness_signal::heartbeat) == 2);
    REQUIRE(static_cast<std::uint8_t>(liveliness_signal::session) == 4);

    const liveliness_signal fused = liveliness_signal::awareness | liveliness_signal::session;
    REQUIRE((fused & liveliness_signal::session) != liveliness_signal::none);
    REQUIRE((fused & liveliness_signal::awareness) != liveliness_signal::none);
    REQUIRE((fused & liveliness_signal::heartbeat) == liveliness_signal::none);
}

TEST_CASE("peer_liveliness types verdict names exactly alive and lost", "[io][peer_liveliness]")
{
    REQUIRE(static_cast<std::uint8_t>(liveliness_verdict::alive) == 0);
    REQUIRE(static_cast<std::uint8_t>(liveliness_verdict::lost) == 1);
}

TEST_CASE("peer_liveliness types options default to the carried and swept values", "[io][peer_liveliness]")
{
    const liveliness_options opts;
    REQUIRE(opts.awareness_ttl == std::chrono::seconds(15));
    REQUIRE(opts.heartbeat_interval == std::chrono::milliseconds(100));
    REQUIRE(opts.heartbeat_miss_limit == 5u);
    REQUIRE(opts.policy == combine::any_signal_alive);
}

TEST_CASE("peer_liveliness types combine names the three fusion rules", "[io][peer_liveliness]")
{
    REQUIRE(static_cast<std::uint8_t>(combine::any_signal_alive) == 0);
    REQUIRE(static_cast<std::uint8_t>(combine::session_authoritative) == 1);
    REQUIRE(static_cast<std::uint8_t>(combine::all_required) == 2);
}

TEST_CASE("peer_liveliness types default observer is unobserved and no-op", "[io][peer_liveliness]")
{
    observer base;
    REQUIRE_FALSE(base.observes_liveliness());
    const peer_liveliness_event ev{node_id{}, liveliness_verdict::alive, liveliness_signal::awareness};
    base.on_peer_liveliness(ev); // no observable effect, must not throw
}

TEST_CASE("peer_liveliness types overridden observer round-trips the event unchanged", "[io][peer_liveliness]")
{
    capturing_observer obs;
    REQUIRE(obs.observes_liveliness());

    node_id id{};
    id[0] = std::byte{0x2a};
    const liveliness_signal contributing = liveliness_signal::heartbeat | liveliness_signal::session;
    const peer_liveliness_event ev{id, liveliness_verdict::lost, contributing};

    obs.on_peer_liveliness(ev);

    REQUIRE(obs.last.has_value());
    REQUIRE(obs.last->id == id);
    REQUIRE(obs.last->verdict == liveliness_verdict::lost);
    REQUIRE(obs.last->contributing == contributing);
}
