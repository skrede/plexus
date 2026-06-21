#ifndef HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H
#define HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H

#include <cstdlib>
#include <stdexcept>

namespace plexus::detail {

// The fail-closed abort hook for the no-exceptions build: when the toolchain is
// compiled without exception support (the constrained-target profile sets
// PLEXUS_NO_EXCEPTIONS), a fail-closed contract that would otherwise `throw`
// terminates the process instead. Default is std::abort; later profiles
// override it with a policy-provided handler. It is [[noreturn]] so the
// fail_closed contract below stays unreachable past the call on every build.
[[noreturn]] inline void on_fatal(const char * /*what*/)
{
    std::abort();
}

// The single fail-closed seam. A site that must refuse to continue (a degraded
// RNG, a sub-minimum PSK, a duplicate local serve) calls fail_closed instead of
// `throw`. Under exceptions (the PC/server build) it throws the same
// std::runtime_error the site threw before, so the generated code and the
// observable behavior are byte-identical to the pre-seam throw. Under
// PLEXUS_NO_EXCEPTIONS it routes to the abort hook — fail-closed, never a silent
// continue past the refused contract.
[[noreturn]] inline void fail_closed(const char *what)
{
#if defined(__cpp_exceptions) && !defined(PLEXUS_NO_EXCEPTIONS)
    throw std::runtime_error(what);
#else
    on_fatal(what);
#endif
}

}

#endif
