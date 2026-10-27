#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_WATCH_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_WATCH_H

#include <new>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

// Observes when one heap block is freed: a test arms a watch for the next allocation of a given size and later asks
// whether that block has been freed. A plain build replaces the global operator new/delete, which a program may do
// only once, so exactly one translation unit of an executable includes this header, and never together with
// alloc_counter.h; a sanitizer already defines those operators, so a sanitized build observes through its hooks.

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
    #define PLEXUS_ALLOC_WATCH_SANITIZED 1
#elif defined(__has_feature)
    #if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
        #define PLEXUS_ALLOC_WATCH_SANITIZED 1
    #endif
#endif

namespace plexus::testing {

struct alloc_watch_state
{
    std::atomic<std::size_t> bytes{0};
    std::atomic<bool> at_least{false};
    std::atomic<bool> armed{false};
    std::atomic<std::uintptr_t> block{0};
    std::atomic<bool> freed{false};
};

// The allocation hooks run before main and carry no context of their own, so the watch is one constant-initialized
// object they reach by name.
inline constinit alloc_watch_state alloc_watch{};

// An exact watch also accepts this much over the size, because MSVC STL's std::allocator asks operator new for a
// 4096+ byte request plus a 2 * sizeof(void *) + 31 byte alignment header (_Allocate_manually_vector_aligned).
constexpr std::size_t k_allocator_header_slack = 64;

inline void arm_alloc_watch(std::size_t bytes, bool at_least) noexcept
{
    alloc_watch.block.store(0);
    alloc_watch.freed.store(false);
    alloc_watch.bytes.store(bytes);
    alloc_watch.at_least.store(at_least);
    alloc_watch.armed.store(true);
}

inline void watch_next_alloc(std::size_t bytes) noexcept
{
    arm_alloc_watch(bytes, false);
}

inline void watch_next_alloc_at_least(std::size_t bytes) noexcept
{
    arm_alloc_watch(bytes, true);
}

inline bool watched_allocated() noexcept
{
    return alloc_watch.block.load() != 0;
}

inline bool watched_freed() noexcept
{
    return alloc_watch.freed.load();
}

inline void note_alloc(std::uintptr_t at, std::size_t size) noexcept
{
    if(at == 0 || !alloc_watch.armed.load())
        return;
    const std::size_t want = alloc_watch.bytes.load();
    if(size < want || (!alloc_watch.at_least.load() && size > want + k_allocator_header_slack))
        return;
    bool expected = true;
    if(alloc_watch.armed.compare_exchange_strong(expected, false))
        alloc_watch.block.store(at);
}

inline void note_free(std::uintptr_t at) noexcept
{
    if(at != 0 && at == alloc_watch.block.load())
        alloc_watch.freed.store(true);
}

}

#if defined(PLEXUS_ALLOC_WATCH_SANITIZED)

extern "C" int __sanitizer_install_malloc_and_free_hooks(void (*malloc_hook)(const volatile void *, std::size_t), void (*free_hook)(const volatile void *));

namespace plexus::testing::detail {

inline void alloc_watch_malloc_hook(const volatile void *p, std::size_t size) noexcept
{
    note_alloc(reinterpret_cast<std::uintptr_t>(p), size);
}

inline void alloc_watch_free_hook(const volatile void *p) noexcept
{
    note_free(reinterpret_cast<std::uintptr_t>(p));
}

inline const std::int32_t alloc_watch_hooks_installed = __sanitizer_install_malloc_and_free_hooks(&alloc_watch_malloc_hook, &alloc_watch_free_hook);

}

#else

inline void *plexus_watched_alloc(std::size_t size) noexcept
{
    void *p = std::malloc(size == 0 ? 1 : size);
    plexus::testing::note_alloc(reinterpret_cast<std::uintptr_t>(p), size);
    return p;
}

inline void plexus_watched_free(void *p) noexcept
{
    plexus::testing::note_free(reinterpret_cast<std::uintptr_t>(p));
    std::free(p);
}

void *operator new(std::size_t size)
{
    if(void *p = plexus_watched_alloc(size))
        return p;
    throw std::bad_alloc{};
}
void *operator new[](std::size_t size)
{
    if(void *p = plexus_watched_alloc(size))
        return p;
    throw std::bad_alloc{};
}
void *operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    return plexus_watched_alloc(size);
}
void *operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    return plexus_watched_alloc(size);
}

void operator delete(void *p) noexcept
{
    plexus_watched_free(p);
}
void operator delete[](void *p) noexcept
{
    plexus_watched_free(p);
}
void operator delete(void *p, std::size_t) noexcept
{
    plexus_watched_free(p);
}
void operator delete[](void *p, std::size_t) noexcept
{
    plexus_watched_free(p);
}
void operator delete(void *p, const std::nothrow_t &) noexcept
{
    plexus_watched_free(p);
}
void operator delete[](void *p, const std::nothrow_t &) noexcept
{
    plexus_watched_free(p);
}

#endif

#endif
