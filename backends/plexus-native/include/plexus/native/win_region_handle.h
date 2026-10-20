#ifndef HPP_GUARD_PLEXUS_NATIVE_WIN_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_NATIVE_WIN_REGION_HANDLE_H

#include "plexus/shm/slot_liveness.h"

#include <span>
#include <array>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::native {

class win_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region on Windows. Every
// handle unmaps the view and closes the file-mapping handle on release; a
// moved-from handle nulls its fields so its destructor no-ops. A named mapping
// auto-frees on last-handle-close, so there is no create/attach unlink split. The
// native HANDLEs are held as void* to keep <windows.h> out of this header. A slot
// hold is a named mutex per slot whose only long-lived handle this handle keeps.
class win_region_handle
{
public:
    win_region_handle() noexcept;
    ~win_region_handle();

    win_region_handle(const win_region_handle &)            = delete;
    win_region_handle &operator=(const win_region_handle &) = delete;

    win_region_handle(win_region_handle &&other) noexcept;
    win_region_handle &operator=(win_region_handle &&other) noexcept;

    std::span<std::byte> bytes() const;

    std::size_t size() const noexcept;

    bool valid() const noexcept
    {
        return m_base != nullptr;
    }

    plexus::shm::slot_hold hold_slot(std::uint32_t slot);

    void release_slot(std::uint32_t slot) noexcept;

    bool slot_held_elsewhere(std::uint32_t slot) const;

    // The slots' objects are created one at a time; one already taken makes this busy and closes what this call created.
    plexus::shm::slot_hold hold_all_slots();

    void release_all_slots() noexcept;

    // A named mapping dies with its last handle, so there is no name to remove.
    void unlink_name() noexcept;

private:
    friend class win_shm_region_broker;

    win_region_handle(void *mapping, void *base, std::size_t length, std::string name) noexcept;

    // noexcept + idempotent: nulls the members so a second invocation (the
    // move-assign path) does nothing.
    void reclaim() noexcept;

    void *m_base;
    void *m_mapping;
    std::string m_name;
    std::size_t m_length;
    std::array<void *, plexus::shm::k_liveness_slots> m_slots;
};

}

#endif
