#ifndef HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H
#define HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H

#include <cstdio>
#include <cstdlib>
#include <stdexcept>

// Objects that disagree on the single-executor check hold two bodies of one inline function; MSVC refuses the link.
#if defined(_MSC_VER)
    #if defined(PLEXUS_AFFINITY_CHECKS)
        #pragma detect_mismatch("plexus_affinity_checks", "1")
    #else
        #pragma detect_mismatch("plexus_affinity_checks", "0")
    #endif
#endif

namespace plexus::detail {

[[noreturn]] inline void on_fatal(const char * /*what*/)
{
    std::abort();
}

// Under exceptions, throws std::runtime_error; under PLEXUS_NO_EXCEPTIONS, aborts.
[[noreturn]] inline void fail_closed(const char *what)
{
#if defined(__cpp_exceptions) && !defined(PLEXUS_NO_EXCEPTIONS)
    throw std::runtime_error(what);
#else
    on_fatal(what);
#endif
}

// A call off the driving thread is a bug in the caller, not a condition to recover from, so it aborts rather than going
// through fail_closed.
inline void affinity_trip(const char *member) noexcept
{
    std::fprintf(stderr, "plexus: %s called off the node's executor thread\n", member);
    std::abort();
}

}

#endif
