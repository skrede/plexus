#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SINK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SINK_H

// A pure header-inline decode whose result is discarded gets dead-code-eliminated,
// leaving the harness a single straight-line edge that fuzzes nothing. Escaping the
// result's address through an opaque asm barrier forces full materialization
// (the same technique as benchmark::DoNotOptimize).
template<typename T>
inline void fuzz_consume(const T &value)
{
    __asm__ volatile("" : : "g"(&value) : "memory");
}

#endif
