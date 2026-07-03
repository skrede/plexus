#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_CHILD_MAIN_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_CHILD_MAIN_H

#include "xproc_harness.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <string_view>

// The child-role entry a re-exec test's main() calls BEFORE handing control to
// Catch2. When argv carries the role marker this process was re-exec'd as a child:
// dispatch to the routine registered under the argv key, passing the remaining
// argv (the shared-object names it re-opens BY NAME), and _Exit with its boolean
// as the exit status -- the parent reads that status as child_succeeded. When the
// marker is absent this returns and the caller proceeds to the normal session.

namespace plexus::testing {

// std::_Exit terminates without running static destructors, so a child routine
// that must leave a named object live (e.g. the crashed-creator orphan
// simulation) can _Exit from within its own body while the owning handle is still
// in scope; a routine that has nothing to preserve may simply return its boolean.
inline void xproc_child_main(int argc, char **argv)
{
    if(argc < 3 || std::string_view(argv[1]) != k_xproc_role_marker)
        return;

    const std::string key = argv[2];
    const std::vector<std::string> args(argv + 3, argv + argc);

    const auto &registry = xproc_child_registry();
    const auto it        = registry.find(key);
    const bool ok        = it != registry.end() && it->second(args);
    std::_Exit(ok ? 0 : 1);
}

}

#endif
