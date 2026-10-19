#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_COUNTER_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_COUNTER_H

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>

// Allocation counter for the no-hot-path-alloc gate: a test snapshots alloc_count(), runs a steady-state loop and
// asserts a zero delta. A plain build replaces the global operator new/delete, which a program may do only once, so
// exactly one translation unit of an executable includes this header; a sanitizer already defines those operators,
// so a sanitized build counts through the sanitizer's allocator hook instead.
// The plain tree counts the replaced operator new calls; the sanitized tree counts every allocation the hook sees,
// including ones that never pass through operator new, so a zero delta on the sanitized tree is the stricter claim.

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define PLEXUS_ALLOC_COUNTER_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define PLEXUS_ALLOC_COUNTER_SANITIZED 1
#endif
#endif

namespace plexus::testing {

inline std::atomic<std::size_t> &alloc_counter_storage() noexcept
{
    static std::atomic<std::size_t> counter{0};
    return counter;
}

inline std::size_t alloc_count() noexcept
{
    return alloc_counter_storage().load(std::memory_order_relaxed);
}

inline void reset_alloc_count() noexcept
{
    alloc_counter_storage().store(0, std::memory_order_relaxed);
}

}

#if defined(PLEXUS_ALLOC_COUNTER_SANITIZED)

// compiler-rt sanitizer_common allocator interface; the hook fires on the
// program's own malloc/free, never on the sanitizer's internal allocations.
extern "C" int __sanitizer_install_malloc_and_free_hooks(
    void (*malloc_hook)(const volatile void *, std::size_t),
    void (*free_hook)(const volatile void *));

namespace plexus::testing::detail {

inline void counting_malloc_hook(const volatile void *, std::size_t) noexcept
{
    alloc_counter_storage().fetch_add(1, std::memory_order_relaxed);
}

inline void counting_free_hook(const volatile void *) noexcept
{
}

inline const int alloc_hooks_installed =
    __sanitizer_install_malloc_and_free_hooks(&counting_malloc_hook, &counting_free_hook);

}

#else

inline void *plexus_counting_alloc(std::size_t size)
{
    plexus::testing::alloc_counter_storage().fetch_add(1, std::memory_order_relaxed);
    if(void *p = std::malloc(size == 0 ? 1 : size))
        return p;
    throw std::bad_alloc{};
}

void *operator new(std::size_t size)
{
    return plexus_counting_alloc(size);
}
void *operator new[](std::size_t size)
{
    return plexus_counting_alloc(size);
}
void *operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    plexus::testing::alloc_counter_storage().fetch_add(1, std::memory_order_relaxed);
    return std::malloc(size == 0 ? 1 : size);
}
void *operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    plexus::testing::alloc_counter_storage().fetch_add(1, std::memory_order_relaxed);
    return std::malloc(size == 0 ? 1 : size);
}

void operator delete(void *p) noexcept
{
    std::free(p);
}
void operator delete[](void *p) noexcept
{
    std::free(p);
}
void operator delete(void *p, std::size_t) noexcept
{
    std::free(p);
}
void operator delete[](void *p, std::size_t) noexcept
{
    std::free(p);
}
void operator delete(void *p, const std::nothrow_t &) noexcept
{
    std::free(p);
}
void operator delete[](void *p, const std::nothrow_t &) noexcept
{
    std::free(p);
}

#endif

#endif
