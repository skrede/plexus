#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEM_SUPPORT_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEM_SUPPORT_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/shm/macos/sem_notifier.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/muxify.h"

#include <asio/io_context.hpp>

#include <string>
#include <optional>
#include <string_view>

#include <semaphore.h>

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

// Mirrors the name the semaphore notifier derives privately from its region.
inline std::string sem_name_for(std::string_view region)
{
    return "/pxn." + std::string(region);
}

// Unlinked on destruction so a killed waiter leaves no semaphore behind.
class scoped_sem_name
{
public:
    explicit scoped_sem_name(std::string_view region)
            : m_name(sem_name_for(region))
    {
    }

    scoped_sem_name(const scoped_sem_name &)            = delete;
    scoped_sem_name &operator=(const scoped_sem_name &) = delete;

    ~scoped_sem_name()
    {
        ::sem_unlink(m_name.c_str());
    }

    const std::string &str() const noexcept
    {
        return m_name;
    }

private:
    std::string m_name;
};

}

#endif
