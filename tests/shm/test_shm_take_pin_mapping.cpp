#include "support/noop_notifier.h"
#include "support/region_names.h"
#include "support/delaying_broker.h"

#include "plexus/shm/detail/shm_ring_mapping.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::acquire_result;
using plexus::shm::shm_slot_owner;
using plexus::shm::ring_direction;
using plexus::shm::slot_subscriber;
using plexus::shm::taken_message;
using plexus::testing::delaying_broker;
using counting_registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;
using native_registry   = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, plexus::testing::noop_notifier>;
using kept_view         = std::optional<plexus::wire_bytes<shm_slot_owner>>;
using counted_mapping   = plexus::shm::detail::ring_mapping_of<delaying_broker::region_handle>;

const std::string k_fqn = "topic.take_pin." + std::to_string(plexus::testing::process_id());

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

bool reads_payload(std::span<const std::byte> bytes)
{
    return bytes.size() == k_payload.size() && std::equal(k_payload.begin(), k_payload.end(), bytes.begin());
}

bool reads_payload(const kept_view &kept)
{
    return reads_payload(std::span<const std::byte>(kept->data(), kept->size()));
}

// A ring over the counting broker's regions whose mapping block holds both handles, as a registry entry's does.
struct pinned_ring
{
    explicit pinned_ring(delaying_broker &broker)
            : mapping(std::make_shared<counted_mapping>())
            , ring()
    {
        REQUIRE(broker.create("pinned", plexus::shm::control_region_bytes(16), {}, mapping->control) == plexus::shm::region_status::ok);
        REQUIRE(broker.create("pinned.s", plexus::shm::slab_region_bytes(16, 64), {}, mapping->slab) == plexus::shm::region_status::ok);
        REQUIRE(plexus::shm::broadcast_ring::create(mapping->control.bytes(), mapping->slab.bytes(), 16, 64, plexus::node_id{}, ring) == loan_status::ok);
        REQUIRE(ring.claim_cursor(0));
    }

    std::shared_ptr<counted_mapping> mapping;
    plexus::shm::broadcast_ring ring;
};

}

TEST_CASE("take_pin_mapping a message held past its entry's teardown keeps the mapping alive", "[shm][take_pin_mapping]")
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

TEST_CASE("take_pin_mapping a message held past its entry's teardown reads its payload over a real mapping", "[shm][take_pin_mapping]")
{
    plexus::native::shm_region_broker broker;
    const std::string name = plexus::testing::region_name(k_fqn, plexus::node_id{});
    native_registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    kept_view kept = publish_and_keep(reg);

    release_both(reg);
    plexus::native::shm_region_handle probe;
    CHECK(broker.attach(name, probe) == plexus::shm::region_status::not_found);
    CHECK(reads_payload(kept));
    kept.reset();
}

TEST_CASE("take_pin_mapping a moved-from view releases nothing", "[shm][take_pin_mapping]")
{
    delaying_broker broker;
    counting_registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    kept_view kept = publish_and_keep(reg);
    release_both(reg);
    const std::size_t mapped = broker.mapped();

    kept_view moved{std::move(*kept)};
    kept.reset();
    CHECK(broker.mapped() == mapped);
    CHECK(reads_payload(moved));
    moved.reset();
    CHECK(broker.mapped() == 0);
}

TEST_CASE("take_pin_mapping a moved-from taken message releases neither its slot nor its mapping", "[shm][take_pin_mapping]")
{
    delaying_broker broker;
    pinned_ring pinned{broker};
    plexus::testing::noop_notifier wake;
    std::optional<slot_subscriber> reader{std::in_place, pinned.ring, slot_subscriber::adopt_cursor, 0u, slot_subscriber::default_spin_budget, pinned.mapping};
    plexus::shm::shm_channel<plexus::testing::noop_notifier> channel{pinned.ring, wake, plexus::io::reliability::reliable, plexus::io::congestion::block};
    REQUIRE(channel.send(k_payload) == loan_status::ok);
    taken_message taken;
    REQUIRE(reader->take(taken) == loan_status::ok);
    std::atomic<std::uint32_t> &pins = pinned.ring.refcount_at(0);

    taken_message moved{std::move(taken)};
    taken = taken_message{};
    reader.reset();
    pinned.mapping.reset();
    CHECK(pins.load() == 1);
    CHECK(broker.mapped() == 2);
    CHECK(reads_payload(moved.bytes()));

    moved = taken_message{};
    CHECK(pins.load() == 0);
    CHECK(broker.mapped() == 0);
}
