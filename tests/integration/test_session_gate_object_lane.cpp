#include "test_session_gate_common.h"

#include "plexus/io/object_carrier.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

using namespace session_gate_fixture;
using plexus::io::loan_slot;
using plexus::io::object_carrier;

namespace {

struct counted_slot
{
    loan_slot slot;
    std::uint32_t releases;
};

void count_release(loan_slot *s)
{
    ++reinterpret_cast<counted_slot *>(s)->releases;
}

// The sender keeps its own reference across the send, so the slot drops to zero only once the session has released the
// reference the channel handed it.
void send_object(lone_session &f, counted_slot &held)
{
    held.slot = loan_slot{.object = &held.releases, .refs = 1, .release = &count_release};
    const object_carrier carrier{.topic_hash = wire::fqn_topic_hash(k_topic), .type_tag = 0, .native_key = nullptr, .sequence = 0, .source_timestamp = 0, .slot = &held.slot};
    f.peer_channel->send_object(carrier);
    f.ex.drain();
    plexus::io::release(carrier);
}

}

TEST_CASE("session gate object lane: an object before completion is not routed", "[integration][session_gate][!shouldfail]")
{
    pre_handshake_acceptor f;
    std::vector<std::string> routed;
    f.subject->on_object_route([&routed](std::string_view fqn, const object_carrier &) { routed.emplace_back(fqn); });
    counted_slot held{};

    send_object(f, held);

    CHECK(routed.empty());
    CHECK(held.releases == 1);
    CHECK_FALSE(f.subject->is_complete());
}

TEST_CASE("session gate object lane: an object on an established session is routed", "[integration][session_gate]")
{
    established_acceptor f;
    REQUIRE(f.subject->is_complete());
    std::vector<std::string> routed;
    f.subject->on_object_route([&routed](std::string_view fqn, const object_carrier &) { routed.emplace_back(fqn); });
    counted_slot held{};

    send_object(f, held);

    CHECK(routed == std::vector<std::string>{std::string{k_topic}});
    CHECK(held.releases == 1);
}
