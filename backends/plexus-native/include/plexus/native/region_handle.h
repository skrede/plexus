#ifndef HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_NATIVE_REGION_HANDLE_H

#include <span>
#include <string>
#include <cstddef>

namespace plexus::native {

class posix_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region. Every handle
// always munmaps; a create handle owns the name and unlinks it on release, an
// attach handle never unlinks. A moved-from handle nulls its fields so its
// destructor no-ops. The base is page-aligned (mmap guarantees it), hence
// >=8-aligned for the ring sub-allocator mapped over it.
class region_handle
{
public:
    region_handle() = default;
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

private:
    friend class posix_shm_region_broker;

    region_handle(int fd, void *base, std::size_t length, std::string name, bool owns_name) noexcept;

    // noexcept + idempotent: nulls the members so a second invocation (the
    // move-assign path) does nothing.
    void reclaim() noexcept;

    int m_fd{-1};
    void *m_base{nullptr};
    std::size_t m_length{0};
    std::string m_name;
    bool m_owns_name{false};
};

}

#endif
