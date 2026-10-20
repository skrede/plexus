#include "support/noop_notifier.h"
#include "support/delaying_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <optional>
#include <algorithm>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::acquire_result;
using plexus::shm::shm_slot_owner;
using plexus::shm::ring_direction;
using plexus::testing::delaying_broker;
using counting_registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;
using kept_view         = std::optional<plexus::wire_bytes<shm_slot_owner>>;

const std::string k_fqn = "topic.take_pin";

constexpr std::array<std::byte, 8> k_payload{std::byte{0x50}, std::byte{0x4c}, std::byte{0x58}, std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}, std::byte{0x05}};

template<typename Registry>
kept_view publish_and_keep(Registry &reg)
{
    kept_view kept;
    REQUIRE(reg.acquire(k_fqn, ring_direction::request, plexus::node_id{}, ring_role::producer, 64) != acquire_result::failed);
    REQUIRE(reg.acquire(k_fqn, ring_direction::request, plexus::node_id{}, ring_role::consumer, 0) != acquire_result::failed);
    REQUIRE(reg.channel_for(k_fqn, ring_direction::request, plexus::node_id{})->send(k_payload) == loan_status::ok);
    reg.set_consumer_sink(k_fqn, ring_direction::request, plexus::node_id{}, [&kept](plexus::wire_bytes<shm_slot_owner> wb) { kept.emplace(std::move(wb)); });
    REQUIRE(kept.has_value());
    reg.clear_consumer_sink(k_fqn, ring_direction::request, plexus::node_id{});
    return kept;
}

template<typename Registry>
void release_both(Registry &reg)
{
    reg.release(k_fqn, ring_direction::request, plexus::node_id{}, ring_role::consumer);
    reg.release(k_fqn, ring_direction::request, plexus::node_id{}, ring_role::producer);
    REQUIRE(reg.live_count() == 0);
}

bool reads_payload(const kept_view &kept)
{
    return kept->size() == k_payload.size() && std::equal(k_payload.begin(), k_payload.end(), kept->data());
}

}

TEST_CASE("take_pin_mapping a message held past its entry's teardown keeps the mapping alive", "[shm][take_pin_mapping][!shouldfail]")
{
    delaying_broker broker;
    counting_registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    kept_view kept = publish_and_keep(reg);

    release_both(reg);
    REQUIRE(broker.mapped() > 0);
    CHECK(reads_payload(kept));

    kept.reset();
    REQUIRE(broker.mapped() == 0);
}
