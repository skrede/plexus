#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEM_SUPPORT_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEM_SUPPORT_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/shm/macos/sem_notifier.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/muxify.h"

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>
#include <optional>
#include <string_view>

namespace shm_harness {

using sem_policy   = plexus::muxify<plexus::asio::asio_policy>;
using sem_notifier = plexus::asio::shm::sem_notifier<sem_policy>;
using sem_registry = plexus::shm::shm_topic_registry<plexus::native::posix_shm_region_broker, sem_notifier>;

// The semaphore notifier compiled on Linux, so its cross-process wake protocol runs where macOS cannot.
struct posix_sem_leg
{
    using registry = sem_registry;

    static registry::notifier_binder binder(::asio::io_context &io)
    {
        return [&io](std::optional<sem_notifier> &slot, plexus::shm::control_header_t &header, std::string_view region) { slot.emplace(io, header, region); };
    }
};

inline bool wait_parked(const plexus::shm::control_header_t &header, std::uint32_t slot)
{
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(header.park[slot].load(std::memory_order_seq_cst) != plexus::shm::k_park_parked)
    {
        if(std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

}

#endif
