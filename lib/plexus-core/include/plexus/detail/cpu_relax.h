#ifndef HPP_GUARD_PLEXUS_DETAIL_CPU_RELAX_H
#define HPP_GUARD_PLEXUS_DETAIL_CPU_RELAX_H

#if defined(__x86_64__) || defined(__i386__)
    #include <immintrin.h>
#elif defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86) || defined(_M_ARM64))
    #include <intrin.h>
#endif

namespace plexus::detail {

// A spin-loop pause hint (PAUSE on x86, YIELD on aarch64, no-op elsewhere). NOT a
// scheduler yield: the spin stays on-core. MSVC spells x86 as _M_X64/_M_IX86 and ARM64 as
// _M_ARM64 (it does not define __x86_64__/__aarch64__), so it needs its own arm.
inline void cpu_relax() noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#elif defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
    _mm_pause();
#elif defined(_MSC_VER) && defined(_M_ARM64)
    __yield();
#endif
}

}

#endif
