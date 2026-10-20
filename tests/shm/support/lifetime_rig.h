#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LIFETIME_RIG_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LIFETIME_RIG_H

#include "support/bus_notifier.h"
#include "support/region_names.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/testing/platform.h"

#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::testing {

using native_broker = plexus::native::shm_region_broker;

// One process's registry on a topic's ring of the test publisher: it records what it reads and every reason it reports.
template<typename Broker>
class basic_ring_holder
{
public:
    basic_ring_holder(wake_bus &bus, std::string fqn)
            : m_broker()
            , m_fqn(std::move(fqn))
            , m_received()
            , m_reasons()
            , m_reg(m_broker, io::reliability::reliable, io::congestion::block,
                    [&bus](std::optional<bus_notifier> &slot, shm::control_header_t &, std::string_view) { slot.emplace(bus); })
    {
        m_reg.on_fallback([this](io::fallback_reason reason) { m_reasons.push_back(reason); });
    }

    shm::acquire_result acquire(shm::ring_role role, std::uint32_t max_payload = 64, std::uint32_t capacity = 0)
    {
        const shm::acquire_result got = m_reg.acquire(m_fqn, shm::ring_direction::request, test_publisher(), role, max_payload, io::ring_geometry_mode::reliable_preserving, capacity);
        if(got != shm::acquire_result::failed && role != shm::ring_role::producer)
            m_reg.set_consumer_sink(m_fqn, shm::ring_direction::request, test_publisher(), [this](wire_bytes<shm::shm_slot_owner> wb) { m_received.push_back(value_of(wb)); });
        return got;
    }

    void release(shm::ring_role role)
    {
        if(role != shm::ring_role::producer)
            m_reg.clear_consumer_sink(m_fqn, shm::ring_direction::request, test_publisher());
        m_reg.release(m_fqn, shm::ring_direction::request, test_publisher(), role);
    }

    shm::loan_status send(std::uint32_t value)
    {
        auto *channel = m_reg.channel_for(m_fqn, shm::ring_direction::request, test_publisher());
        if(channel == nullptr)
            return shm::loan_status::unknown;
        return channel->send(std::as_bytes(std::span<const std::uint32_t, 1>(&value, 1)));
    }

    shm::acquire_bound last_bound() const noexcept
    {
        return m_reg.last_acquire_failure().bound;
    }

    bool lost() const noexcept
    {
        return std::ranges::find(m_reasons, io::fallback_reason::waiter_lost) != m_reasons.end();
    }

    const std::vector<std::uint32_t> &received() const noexcept
    {
        return m_received;
    }

    Broker &broker() noexcept
    {
        return m_broker;
    }

private:
    Broker m_broker;
    std::string m_fqn;
    std::vector<std::uint32_t> m_received;
    std::vector<io::fallback_reason> m_reasons;
    shm::shm_topic_registry<Broker, bus_notifier> m_reg;

    static std::uint32_t value_of(const wire_bytes<shm::shm_slot_owner> &wb) noexcept
    {
        std::uint32_t value = 0;
        std::memcpy(&value, wb.data(), std::min(wb.size(), sizeof(value)));
        return value;
    }
};

using ring_holder = basic_ring_holder<native_broker>;

// The topic's ring as another process maps it, through handles of its own; it removes the ring's names on exit.
struct ring_view
{
    explicit ring_view(std::string topic)
            : broker()
            , fqn(std::move(topic))
            , name(region_name(fqn, test_publisher()))
            , control()
            , slab()
            , ring()
    {
    }

    ~ring_view()
    {
        broker.unlink(name + ".s");
        broker.unlink(name);
    }

    bool named()
    {
        native_broker::region_handle probe;
        return broker.attach(name, probe) == shm::region_status::ok;
    }

    bool map()
    {
        return broker.attach(name, control) == shm::region_status::ok && broker.attach(name + ".s", slab) == shm::region_status::ok &&
                shm::broadcast_ring::attach(control.bytes(), slab.bytes(), test_publisher(), ring) == shm::loan_status::ok;
    }

    native_broker broker;
    std::string fqn;
    std::string name;
    native_broker::region_handle control;
    native_broker::region_handle slab;
    shm::broadcast_ring ring;
};

inline std::string lifetime_fqn(const char *tag)
{
    return std::string("ring_lifetime.") + tag + "." + std::to_string(process_id());
}

}

#endif
