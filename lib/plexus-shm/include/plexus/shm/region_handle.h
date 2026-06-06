#ifndef HPP_GUARD_PLEXUS_SHM_REGION_HANDLE_H
#define HPP_GUARD_PLEXUS_SHM_REGION_HANDLE_H

#include <span>
#include <string>
#include <cstddef>

namespace plexus::shm {

class posix_shm_region_broker;

// Move-only RAII owner of a single mapped shared-memory region. The OS
// primitives (fd, base pointer) are private and never cross the boundary; the
// only public payload accessor is a writable std::span<std::byte>. The base is
// page-aligned by construction (mmap guarantees it), hence >=8-aligned for the
// ring sub-allocator that maps over it.
//
// Lifetime asymmetry (the correctness-critical invariant): every handle owns
// its own mapping and always munmaps. A handle produced by create owns the
// name and unlinks it on release; a handle produced by attach must never
// unlink. A moved-from handle nulls its fields so its destructor no-ops.
class region_handle
{
public:
    region_handle() = default;
    ~region_handle();

    region_handle(const region_handle &) = delete;
    region_handle &operator=(const region_handle &) = delete;

    region_handle(region_handle &&other) noexcept;
    region_handle &operator=(region_handle &&other) noexcept;

    // Writable view over the mapped region. Calling this on an empty or
    // moved-from handle is caller misuse, not a recoverable error.
    [[nodiscard]] std::span<std::byte> bytes() const;

    // Mapped capacity in bytes (page-rounded at creation).
    [[nodiscard]] std::size_t size() const noexcept;

    // True for a live mapping, false for an empty or moved-from handle.
    [[nodiscard]] bool valid() const noexcept { return m_base != nullptr; }

private:
    friend class posix_shm_region_broker;

    region_handle(int fd, void *base, std::size_t length, std::string name, bool owns_name) noexcept;

    // Always munmaps, closes the fd, and unlinks the name only when this handle
    // owns it. noexcept + idempotent: nulls the members so a second invocation
    // (the move-assign path) does nothing.
    void reclaim() noexcept;

    int m_fd{-1};
    void *m_base{nullptr};
    std::size_t m_length{0};
    std::string m_name;
    bool m_owns_name{false};
};

}

#endif
