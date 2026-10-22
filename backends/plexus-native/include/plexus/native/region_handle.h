#ifndef HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H

#include "plexus/native/detail/posix_slot_lock.h"

#include "plexus/shm/slot_liveness.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::native {

class posix_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region. Every handle
// munmaps on release and never removes the name; unlink_name does, leaving the
// mapping valid. A moved-from handle nulls its fields so its
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

    plexus::shm::slot_hold hold_all_slots() noexcept;

    void release_all_slots() noexcept;

    // The name goes before its guard: the sidecar lock file is removed after the name, while its lock is still held, so no
    // other remover can act on the name from a lock file that is gone.
    void unlink_name() noexcept;

private:
    friend class posix_shm_region_broker;

    region_handle(int fd, void *base, std::size_t length, std::string name) noexcept;

    // noexcept + idempotent: nulls the members so a second invocation (the
    // move-assign path) does nothing.
    void reclaim() noexcept;

    int m_fd;
    void *m_base;
    std::string m_name;
    std::size_t m_length;
    mutable detail::sidecar_lock_file m_sidecar;
};

}

#endif
