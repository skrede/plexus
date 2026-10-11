#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_SHM_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_SHM_NOTIFIER_H

#if defined(_WIN32)
    #include "plexus/asio/shm/windows/event_notifier.h"
#elif defined(__APPLE__)
    #include "plexus/asio/shm/macos/sem_notifier.h"
#else
    #include "plexus/asio/shm/linux/ring_notifier.h"
#endif

#include <asio/io_context.hpp>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::asio::shm {

#if defined(_WIN32)
template<typename Policy>
using shm_notifier = event_notifier<Policy>;
#elif defined(__APPLE__)
template<typename Policy>
using shm_notifier = sem_notifier<Policy>;
#else
template<typename Policy>
using shm_notifier = ring_notifier<Policy>;
#endif

// The Linux notifiers wait on the in-region word itself; only the named-object platforms derive
// their kernel object from the region name.
template<typename Policy>
void emplace_shm_notifier(std::optional<shm_notifier<Policy>> &slot, ::asio::io_context &io, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park,
                          std::string_view region_name)
{
#if defined(_WIN32) || defined(__APPLE__)
    slot.emplace(io, word, park, region_name);
#else
    (void)region_name;
    slot.emplace(io, word, park);
#endif
}

}

#endif
