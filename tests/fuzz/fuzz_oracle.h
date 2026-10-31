#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_ORACLE_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_ORACLE_H

#include <cstdio>
#include <cstdlib>
#include <string_view>

// abort() rather than assert() so the check survives NDEBUG and libFuzzer keeps the offending input as an artifact.
inline void fuzz_require(bool condition, std::string_view contract)
{
    if(condition)
        return;
    std::fprintf(stderr, "plexus fuzz oracle violated: %.*s\n", static_cast<int>(contract.size()), contract.data());
    std::abort();
}

// The planted binary inverts one oracle leg, so a run of it proves that a violation fails the process.
#ifdef PLEXUS_FUZZ_PLANT_VIOLATION
inline constexpr bool planted_violation = true;
#else
inline constexpr bool planted_violation = false;
#endif

#endif
