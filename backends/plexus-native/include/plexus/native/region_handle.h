#ifndef HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H

#include "plexus/shm/slot_liveness.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::native {

class posix_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region. Every handle
// always munmaps; a create handle owns the name and unlinks it on release, an
// attach handle never unlinks. A moved-from handle nulls its fields so its
// destructor no-ops. The base is page-aligned (mmap guarantees it), hence
// >=8-aligned for the ring sub-allocator mapped over it. Slot holds are open-file-description locks on the handle's
// own descriptor, so two handles are two holders even in one process.
class region_handle
{
public:
    region_handle() noexcept;
    ~region_handle();

    region_handle(const region_handle &)            = delete;
    region_handle &operator=(const region_handle &) = delete;

    region_handle(region_handle &&other) noexcept;
    region_handle &operator=(region_handle &&other) noexcept;

    std::span<std::byte> bytes() const;

    std::size_t size() const noexcept;

    bool valid() const noexcept
    {
        return m_base != nullptr;
    }

    plexus::shm::slot_hold hold_slot(std::uint32_t slot) noexcept;

    void release_slot(std::uint32_t slot) noexcept;

    bool slot_held_elsewhere(std::uint32_t slot) const noexcept;

private:
    friend class posix_shm_region_broker;

    region_handle(int fd, void *base, std::size_t length, std::string name, bool owns_name) noexcept;

    // noexcept + idempotent: nulls the members so a second invocation (the
    // move-assign path) does nothing.
    void reclaim() noexcept;

    int m_fd;
    void *m_base;
    bool m_owns_name;
    std::string m_name;
    std::size_t m_length;
    // Opened on the first slot call on a host whose shared-memory descriptors take no record lock.
    mutable int m_sidecar_fd;
};

}

#endif
