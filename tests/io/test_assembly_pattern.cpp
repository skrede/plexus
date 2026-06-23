// The assembly-pattern doc-header compile guard: a compile-only unit that pulls the
// documentation-only io/assembly_pattern.h into the build graph so the header cannot
// silently fail to parse (no doc-header bit-rot). The header carries no declarations —
// it is a header-comment contract for how a datagram backend assembles the core blocks
// around its pump — so this unit only proves it compiles and that the namespace it
// opens is well-formed. No socket, no backend — header-only core, plexus::plexus only.

#include "plexus/datagram/assembly_pattern.h"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("assembly_pattern doc header is in the compile graph", "[io][assembly_pattern]")
{
    // The header declares nothing; reaching here proves it parsed under the real compiler.
    SUCCEED("assembly_pattern.h compiled");
}
