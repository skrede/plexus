#include "notifier_legs.h"

#include "support/region_names.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_mux_member.h"
#include "plexus/shm/shm_byte_channel.h"
#include "plexus/shm/fallback_notifier.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace {

using plexus::shm::arm_status;
using plexus::shm::ring_role;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using lost_notifier = plexus::shm::fallback_notifier<plexus::testing::demoting_primary<plexus::asio::asio_policy>, plexus::testing::denied_floor>;
using registry      = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, lost_notifier>;
using lost_member   = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, lost_notifier>;
using consumer      = plexus::shm::shm_companion_consumer<plexus::native::shm_region_broker, lost_notifier>;

registry::notifier_binder lost_binder(::asio::io_context &io)
{
    return [&io](std::optional<lost_notifier> &slot, plexus::shm::control_header_t &header, std::string_view name) { slot.emplace(io, header, name); };
}

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_loss.") + tag + "." + std::to_string(plexus::testing::process_id());
}

bool region_exists(const std::string &fqn)
{
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle handle;
    return broker.attach(plexus::testing::region_name(fqn, plexus::node_id{}), handle) == plexus::shm::region_status::ok;
}

}

TEST_CASE("registry_loss a waiter lost at runtime fails every acquire of its key until the last release", "[shm][registry_loss]")
{
    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, lost_binder(io)};
    const std::string fqn = fresh_fqn("latch");

    REQUIRE(reg.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::both, 0) == acquire_result::created);
    io.run();

    CHECK(reg.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::both, 0) == acquire_result::failed);
    CHECK(reg.last_acquire_failure().bound == plexus::shm::acquire_bound::notifier);
    CHECK(reg.last_acquire_failure().arm == arm_status::denied);
    CHECK(reg.channel_for(fqn, ring_direction::request, plexus::node_id{}) == nullptr);

    reg.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::both);
    CHECK(reg.live_count() == 0);
    CHECK_FALSE(region_exists(fqn));

    REQUIRE(reg.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::both, 0) == acquire_result::created);
    reg.release(fqn, ring_direction::request, plexus::node_id{}, ring_role::both);
}

TEST_CASE("registry_loss each holder watching a lost waiter is told once and the last release unlinks the ring", "[shm][registry_loss]")
{
    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    lost_member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, lost_binder(io)};
    const std::string fqn = fresh_fqn("holders");

    std::uint32_t told = 0;
    std::unique_ptr<lost_member::channel_type> send;
    std::unique_ptr<consumer> receive;
    auto drop_send = [&]
    {
        ++told;
        send.reset();
    };
    auto drop_receive = [&]
    {
        ++told;
        receive.reset();
    };
    send    = shm.mint_companion(fqn, drop_send);
    receive = shm.mint_receive_companion(fqn, shm.self_id(), [](std::span<const std::byte>) {}, drop_receive);
    REQUIRE(send);
    REQUIRE(receive);
    io.run();

    CHECK(told == 2);
    CHECK(shm.registry().live_count() == 0);
    CHECK_FALSE(region_exists(fqn));
    CHECK(shm.mint_companion(fqn, nullptr) != nullptr);
}

TEST_CASE("registry_loss a holder an earlier told holder released is not told", "[shm][registry_loss]")
{
    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    lost_member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, lost_binder(io)};
    const std::string fqn = fresh_fqn("released");

    std::uint32_t send_told    = 0;
    std::uint32_t receive_told = 0;
    std::unique_ptr<lost_member::channel_type> send;
    std::unique_ptr<consumer> receive;
    auto drop_both = [&]
    {
        ++send_told;
        send.reset();
        receive.reset();
    };
    send    = shm.mint_companion(fqn, drop_both);
    receive = shm.mint_receive_companion(fqn, shm.self_id(), [](std::span<const std::byte>) {}, [&receive_told] { ++receive_told; });
    io.run();

    CHECK(send_told == 1);
    CHECK(receive_told == 0);
    CHECK(shm.registry().live_count() == 0);
}

TEST_CASE("registry_loss a dialed channel no owner watches closes when its waiter is lost", "[shm][registry_loss]")
{
    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    lost_member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, lost_binder(io)};
    const std::string fqn = fresh_fqn("dialed");

    std::unique_ptr<lost_member::channel_type> dialed;
    shm.on_dialed([&dialed](std::unique_ptr<lost_member::channel_type> ch, const plexus::io::endpoint &) { dialed = std::move(ch); });
    shm.dial(plexus::io::endpoint{"shm", fqn});
    REQUIRE(dialed);
    bool closed = false;
    dialed->on_closed([&closed] { closed = true; });
    io.run();

    CHECK(closed);
    CHECK(shm.registry().live_count() == 0);
    CHECK_FALSE(region_exists(fqn));
}
