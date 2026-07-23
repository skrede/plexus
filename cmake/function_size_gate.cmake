# The function assertion: no function may exceed the conventions.md function
# ceiling (25 lines, comments and blank lines included) or the cognitive-
# complexity ceiling unless it is a registered exception (a function-scoped
# // NOLINTNEXTLINE(readability-function-size) at the site, justified in the
# owning file's EXCEPTIONS.md row). clang-tidy owns this check; cmake cannot.
#
# Scope: the FULL compile DB, not a positional path subset. A header-only
# template body is checked ONLY when a TU that instantiates it is visited, so
# restricting the run to the directly-compiled lib/*/src TUs leaves every
# header-only lib body unseen (a non-user-code suppression that silently passes
# the gate). Visiting the whole DB -- including the test TUs that instantiate
# the library templates -- is the only way to reach those bodies. The
# diagnostics are then scoped back to the library via -header-filter so only a
# lib/plexus or backends/plexus overage can fail the build; test and example
# bodies are out of this gate's scope (their own size is a separate question).
#
# -warnings-as-errors keys on the diagnostic CHECK, not its location, so a bare
# warnings-as-errors run over the full DB would also fail on the test/example
# main-source bodies that -header-filter cannot suppress (header-filter gates
# header locations, never a TU's own main file). This gate therefore runs
# clang-tidy advisory and fails the build itself iff a surviving diagnostic is
# located in a library path -- the same scan-then-FATAL shape as size_gate
# and shm_leak_gate. This file is self-dispatching: included from the build it
# registers an ALL target that re-invokes it via -P with FUNC_GATE_FATAL=ON; a
# direct -P invocation without it stays advisory for probing by hand.

# The library path fragment a surviving diagnostic must match to fail the build:
# Tier-1/2 logic under lib/plexus and Tier-3 mechanisms under backends/plexus.
set(FUNC_GATE_SCOPE "/(lib|backends)/plexus")

if(NOT FUNC_GATE_SCAN)
    # run-clang-tidy drives this gate and runs on the Linux leg only, so the gate is
    # inert off Linux rather than a configure-time FATAL where run-clang-tidy is absent.
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        return()
    endif()
    add_custom_target(function_size_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DFUNC_GATE_SCAN=ON
                -DFUNC_GATE_FATAL=ON
                -DFUNC_GATE_BUILD_DIR=${PROJECT_BINARY_DIR}
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting library functions stay within the size + cognitive-complexity ceilings"
        VERBATIM)
    return()
endif()

# run-clang-tidy drives the per-TU compile DB; without it the gate cannot run.
find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
if(NOT RUN_CLANG_TIDY)
    message(WARNING
        "function size gate: run-clang-tidy not found -- the function-size + "
        "cognitive-complexity gate is NOT active in this tree. Install LLVM's "
        "run-clang-tidy to enforce the function ceiling at build time.")
    return()
endif()

# Run advisory over the WHOLE compile DB so header-only lib bodies are reached
# through the test TUs that instantiate them; -header-filter scopes the emitted
# header diagnostics to the library, -checks restricts the run to the two size
# checks so an unrelated diagnostic never enters the result.
execute_process(
    COMMAND ${RUN_CLANG_TIDY}
            -p ${FUNC_GATE_BUILD_DIR}
            -quiet
            -checks=-*,readability-function-size,readability-function-cognitive-complexity
            "-header-filter=.*/(lib|backends)/plexus.*"
    OUTPUT_VARIABLE _tidy_out
    ERROR_VARIABLE _tidy_err
    RESULT_VARIABLE _tidy_rc)

# Keep only the size/complexity warning lines that point into the library tree;
# a test or example main-source body is out of this gate's scope.
set(_violations "")
string(REPLACE "\n" ";" _lines "${_tidy_out}")
foreach(_line IN LISTS _lines)
    if(_line MATCHES "warning:.*(readability-function-size|readability-function-cognitive-complexity)")
        if(_line MATCHES "${FUNC_GATE_SCOPE}")
            list(APPEND _violations "${_line}")
        endif()
    endif()
endforeach()
list(REMOVE_DUPLICATES _violations)
list(LENGTH _violations _nviol)

if(_nviol EQUAL 0)
    message(STATUS "function size gate: clean (no unsuppressed library function overage)")
    return()
endif()

set(_report "function size gate: ${_nviol} unsuppressed library function overage(s):")
foreach(_v IN LISTS _violations)
    string(STRIP "${_v}" _v)
    string(APPEND _report "\n  - ${_v}")
endforeach()
string(APPEND _report
    "\n(suppress an earned overage with a function-scoped "
    "// NOLINTNEXTLINE(readability-function-size) at the site + an EXCEPTIONS.md entry; "
    "never loosen the ceiling)")

if(FUNC_GATE_FATAL)
    message(FATAL_ERROR "${_report}")
else()
    message(WARNING "${_report}\n(advisory: not yet build-failing)")
endif()
