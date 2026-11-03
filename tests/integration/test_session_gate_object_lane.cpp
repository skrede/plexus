#include "test_session_gate_common.h"

#include "plexus/io/object_carrier.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/peer_session_deliver.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

using namespace session_gate_fixture;
using plexus::io::loan_slot;
using plexus::io::object_carrier;
using plexus::io::detail::drop_cause;
using plexus::io::detail::drop_event;

namespace {

struct counted_slot
{
    loan_slot slot;
    std::uint32_t releases;
};

struct object_probe
{
    std::vector<std::string> routed;
    std::vector<drop_event> drops;
};

void count_release(loan_slot *s)
{
    ++reinterpret_cast<counted_slot *>(s)->releases;
}

object_carrier carrier_for(counted_slot &held, std::uint32_t refs)
{
    held.slot = loan_slot{.object = &held.releases, .refs = refs, .release = &count_release};
    return object_carrier{.topic_hash = wire::fqn_topic_hash(k_topic), .type_tag = 0, .native_key = nullptr, .sequence = 0, .source_timestamp = 0, .slot = &held.slot};
}

// The sender keeps its own reference across the send, so the slot drops to zero only once the session has released the
// reference the channel handed it.
void send_object(lone_session &f, counted_slot &held)
{
    const object_carrier carrier = carrier_for(held, 1);
    f.peer_channel->send_object(carrier);
    f.ex.drain();
    plexus::io::release(carrier);
}

// Teardown closes the session's channel, which then releases objects itself, so the object lane is handed the carrier
// directly with the reference a channel would have passed.
void hand_object(lone_session &f, counted_slot &held)
{
    const object_carrier carrier = carrier_for(held, 2);
    plexus::io::detail::deliver_session_object(*f.subject, carrier);
    plexus::io::release(carrier);
}

void watch(lone_session &f, object_probe &probe)
{
    f.subject->on_object_route([&probe](std::string_view fqn, const object_carrier &) { probe.routed.emplace_back(fqn); });
    f.subject->on_frame_dropped([&probe](const drop_event &e) { probe.drops.push_back(e); });
}

void check_refused(const object_probe &probe, const counted_slot &held)
{
    CHECK(probe.routed.empty());
    CHECK(held.releases == 1);
    REQUIRE(probe.drops.size() == 1);
    CHECK(probe.drops.front().cause == drop_cause::unestablished);
    CHECK(probe.drops.front().topic_hash == wire::fqn_topic_hash(k_topic));
}
}

TEST_CASE("session gate object lane: an object before completion is not routed", "[integration][session_gate]")
{
    pre_handshake_acceptor f;
    object_probe probe;
    watch(f, probe);
    counted_slot held{};

    send_object(f, held);

    check_refused(probe, held);
    CHECK_FALSE(f.subject->is_complete());
}

TEST_CASE("session gate object lane: an object after teardown is not routed", "[integration][session_gate]")
{
    established_acceptor f;
    f.subject->tear_down();
    f.settle();
    object_probe probe;
    watch(f, probe);
    counted_slot held{};

    hand_object(f, held);

    check_refused(probe, held);
    CHECK_FALSE(f.subject->is_complete());
}

TEST_CASE("session gate object lane: an object on an established session is routed", "[integration][session_gate]")
{
    established_acceptor f;
    REQUIRE(f.subject->is_complete());
    object_probe probe;
    watch(f, probe);
    counted_slot held{};

    send_object(f, held);

    CHECK(probe.routed == std::vector<std::string>{std::string{k_topic}});
    CHECK(probe.drops.empty());
    CHECK(held.releases == 1);
}
