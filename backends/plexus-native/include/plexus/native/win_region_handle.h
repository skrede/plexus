#ifndef HPP_GUARD_PLEXUS_NATIVE_WIN_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_NATIVE_WIN_REGION_HANDLE_H

#include <span>
#include <cstddef>

namespace plexus::native {

class win_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region on Windows. Every
// handle unmaps the view and closes the file-mapping handle on release; a
// moved-from handle nulls its fields so its destructor no-ops. A named mapping
// auto-frees on last-handle-close, so there is no create/attach unlink split. The
// native HANDLEs are held as void* to keep <windows.h> out of this header.
class win_region_handle
{
public:
    win_region_handle() = default;
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

private:
    friend class win_shm_region_broker;

    win_region_handle(void *mapping, void *base, std::size_t length) noexcept;

    // noexcept + idempotent: nulls the members so a second invocation (the
    // move-assign path) does nothing.
    void reclaim() noexcept;

    void *m_mapping{nullptr};
    void *m_base{nullptr};
    std::size_t m_length{0};
};

}

#endif
