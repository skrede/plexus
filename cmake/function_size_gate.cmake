# No library function may exceed the function ceiling (25 lines, comments and blanks included) or the
# cognitive-complexity ceiling unless it carries a function-scoped NOLINTNEXTLINE(readability-function-size) and an
# EXCEPTIONS.md row. The run visits the whole compile database because a header-only template body is checked only
# through a TU that instantiates it, which for the libraries means the test TUs; -header-filter scopes diagnostics
# back to the library. clang-tidy runs advisory and the gate fails itself on a surviving library diagnostic, because
# -warnings-as-errors keys on the check, not its location, and -header-filter cannot suppress a TU's own main file.
# Included from the build, the file registers an ALL target that re-runs it through -P with FUNC_GATE_FATAL=ON; a
# bare -P run stays advisory.
set(FUNC_GATE_SCOPE "/(lib|backends)/plexus")

if(NOT FUNC_GATE_SCAN)
    # run-clang-tidy runs on the Linux leg only, so the gate is inert elsewhere rather than a configure-time FATAL.
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

# FUNC_GATE_TIDY_LOG replays a recorded clang-tidy output so the gate's parsing can be tested against fixed
# diagnostics.
if(FUNC_GATE_TIDY_LOG)
    file(READ "${FUNC_GATE_TIDY_LOG}" _tidy_out)
else()
    find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
    if(NOT RUN_CLANG_TIDY)
        message(WARNING
            "function size gate: run-clang-tidy not found -- the function-size + "
            "cognitive-complexity gate is NOT active in this tree. Install LLVM's "
            "run-clang-tidy to enforce the function ceiling at build time.")
        return()
    endif()

    # -checks keeps an unrelated diagnostic out of the result.
    execute_process(
        COMMAND ${RUN_CLANG_TIDY}
                -p ${FUNC_GATE_BUILD_DIR}
                -quiet
                -checks=-*,readability-function-size,readability-function-cognitive-complexity
                "-header-filter=.*/(lib|backends)/plexus.*"
        OUTPUT_VARIABLE _tidy_out
        ERROR_VARIABLE _tidy_err
        RESULT_VARIABLE _tidy_rc)
endif()

# Warning lines are matched out of the raw output rather than split into a list, because CMake does not split a list
# on ';' inside brackets and clang-tidy's quoted source lines carry unbalanced ones.
set(_violations "")
string(REGEX MATCHALL
    "[^\n]*warning:[^\n]*(readability-function-size|readability-function-cognitive-complexity)[^\n]*"
    _warnings "${_tidy_out}")
foreach(_line IN LISTS _warnings)
    if(_line MATCHES "${FUNC_GATE_SCOPE}")
        list(APPEND _violations "${_line}")
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
