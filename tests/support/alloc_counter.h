#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_COUNTER_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_ALLOC_COUNTER_H

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>

// Translation-unit-local allocation counter for the no-hot-path-alloc gate.
// Including this header in a test TU overrides the global operator new/delete
// (and the array and sized variants) so every heap allocation bumps an atomic
// counter. A test snapshots alloc_count(), runs a steady-state loop, and asserts
// the delta is zero — proving the loop allocated nothing. Header-only and
// self-contained: it does not depend on any plexus target.
//
// Because operator new/delete are replaceable at most once per program, this
// header must be included in exactly one TU of any executable that links it.

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
