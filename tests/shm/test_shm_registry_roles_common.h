#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_TEST_SHM_REGISTRY_ROLES_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_TEST_SHM_REGISTRY_ROLES_COMMON_H

#include "support/region_names.h"
#include "support/delaying_broker.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <functional>
#include <string_view>

namespace registry_roles_fixture {

using plexus::shm::ring_role;
using plexus::shm::arm_status;
using plexus::shm::loan_status;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using plexus::testing::delaying_broker;

constexpr std::uint32_t k_sends = 600;
inline const std::string k_fqn  = "topic.roles";

struct wake_log
{
    bool armed;
    std::uint32_t arms;
};

struct logging_notifier
{
    explicit logging_notifier(wake_log &log) noexcept
            : log(log)
    {
    }

    void signal() noexcept
    {
    }

    arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        log.get().armed = true;
        ++log.get().arms;
        return arm_status::armed;
    }

    void disarm() noexcept
    {
        log.get().armed = false;
    }

    std::reference_wrapper<wake_log> log;
};

using registry = plexus::shm::shm_topic_registry<delaying_broker, logging_notifier>;

// One process's registry over the shared in-memory host, logging whether its waiter is armed.
struct participant
{
    explicit participant(delaying_broker &broker)
            : log{false, 0}
            , reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block,
                  [this](std::optional<logging_notifier> &slot, plexus::shm::control_header_t &, std::string_view) { slot.emplace(log); })
    {
    }

    acquire_result acquire(ring_role role)
    {
        return reg.acquire(k_fqn, ring_direction::request, plexus::node_id{}, role, 64);
    }

    void release(ring_role role)
    {
        reg.release(k_fqn, ring_direction::request, plexus::node_id{}, role);
    }

    bool sends_freely()
    {
        const std::array<std::byte, 8> payload{};
        for(std::uint32_t i = 0; i < k_sends; ++i)
            if(reg.channel_for(k_fqn, ring_direction::request, plexus::node_id{})->send(payload) != loan_status::ok)
                return false;
        return true;
    }

    wake_log log;
    registry reg;
};

// The ring header as any process on the host sees it.
struct header_view
{
    explicit header_view(delaying_broker &broker)
            : control()
            , slab()
            , ring()
    {
        const std::string name = plexus::testing::region_name(k_fqn, plexus::node_id{});
        REQUIRE(broker.attach(name, control) == plexus::shm::region_status::ok);
        REQUIRE(broker.attach(name + ".s", slab) == plexus::shm::region_status::ok);
        REQUIRE(plexus::shm::broadcast_ring::attach(control.bytes(), slab.bytes(), plexus::node_id{}, ring) == loan_status::ok);
    }

    std::uint32_t active_cursors()
    {
        std::uint32_t active = 0;
        for(std::uint32_t i = 0; i < ring.registered_high_water(); ++i)
            active += ring.header().cursors[i].active.load(std::memory_order_acquire);
        return active;
    }

    delaying_broker::region_handle control;
    delaying_broker::region_handle slab;
    plexus::shm::broadcast_ring ring;
};

}

#endif
