#ifndef HPP_GUARD_PLEXUS_DETAIL_CPU_RELAX_H
#define HPP_GUARD_PLEXUS_DETAIL_CPU_RELAX_H

#if defined(__x86_64__) || defined(__i386__)
    #include <immintrin.h>
#endif

namespace plexus::detail {

// A spin-loop pause hint (PAUSE on x86, YIELD on aarch64, no-op elsewhere). NOT a
// scheduler yield: the spin stays on-core.
inline void cpu_relax() noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

}

#endif
