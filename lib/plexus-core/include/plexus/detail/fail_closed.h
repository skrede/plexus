#ifndef HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H
#define HPP_GUARD_PLEXUS_DETAIL_FAIL_CLOSED_H

#include <cstdlib>
#include <stdexcept>

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

}

#endif
