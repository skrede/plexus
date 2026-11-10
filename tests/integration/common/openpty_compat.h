#ifndef HPP_GUARD_TESTS_INTEGRATION_OPENPTY_COMPAT_H
#define HPP_GUARD_TESTS_INTEGRATION_OPENPTY_COMPAT_H

#if defined(__APPLE__)
#include <util.h>
#else
#include <pty.h>
#endif

#endif
