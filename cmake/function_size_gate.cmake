# No library function may exceed the function ceiling (25 lines as it sits on screen, from its signature's first line
# to its closing brace, comments and blanks included) or the cognitive-complexity ceiling unless it carries a
# function-scoped NOLINTNEXTLINE(readability-function-size) and an EXCEPTIONS.md row. clang-tidy checks a header's
# bodies through any TU that includes it, and lib/ is header-only, so the run visits the whole compile database, test
# TUs included, and -header-filter scopes diagnostics back to the library. clang-tidy runs advisory and the gate fails
# itself on a surviving library diagnostic, because -warnings-as-errors keys on the check, not its location, and
# -header-filter cannot suppress a TU's own main file. Included from the build, the file registers an ALL target that
# re-runs it through -P with FUNC_GATE_FATAL=ON; a bare -P run stays advisory.
set(FUNC_GATE_SCOPE "/(lib|backends)/plexus")
set(FUNC_GATE_CEILING 25)

# A bare -P run sets no policy, and the line lists need CMP0007's empty elements.
cmake_policy(VERSION 3.25)
include(${CMAKE_CURRENT_LIST_DIR}/function_size_gate_coverage.cmake)

if(NOT FUNC_GATE_SCAN)
    # run-clang-tidy runs on the Linux leg only, so the gate is inert elsewhere rather than a configure-time FATAL.
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        return()
    endif()
    function_size_gate_library_sources(_library_sources)
    list(JOIN _library_sources "\n" _library_sources)
    file(WRITE ${PROJECT_BINARY_DIR}/function_size_gate_sources.txt "${_library_sources}\n")
    add_custom_target(function_size_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DFUNC_GATE_SCAN=ON
                -DFUNC_GATE_FATAL=ON
                -DFUNC_GATE_BUILD_DIR=${PROJECT_BINARY_DIR}
                -DFUNC_GATE_SOURCES_FILE=${PROJECT_BINARY_DIR}/function_size_gate_sources.txt
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting library functions stay within the size + cognitive-complexity ceilings"
        VERBATIM)
    return()
endif()

include(${CMAKE_CURRENT_LIST_DIR}/function_size_gate_lines.cmake)

if(FUNC_GATE_SOURCES_FILE)
    function_size_gate_check_coverage()
endif()

# clang-tidy exits nonzero on a TU that fails to compile, as the compile-fail tests' TUs do by design, so the exit
# status alone cannot tell a broken run: one is broken when run-clang-tidy itself fails, a clang-tidy dies, a library
# TU cannot be processed, or no function was measured at all.
function(function_size_gate_check_run log rc err)
    file(STRINGS "${log}" _failures REGEX "terminated by signal|^Error while processing .*${FUNC_GATE_SCOPE}")
    file(STRINGS "${log}" _measured REGEX ": note: [0-9]+ lines including" LIMIT_COUNT 1)
    if(NOT rc EQUAL 0 AND NOT err STREQUAL "")
        message(FATAL_ERROR "function size gate: run-clang-tidy failed (exit ${rc}):\n${err}")
    elseif(_failures)
        list(JOIN _failures "\n  " _failures)
        message(FATAL_ERROR "function size gate: clang-tidy did not finish a source:\n  ${_failures}")
    elseif(NOT _measured)
        message(FATAL_ERROR "function size gate: clang-tidy measured no function")
    endif()
endfunction()

set(_diagnostic_regex
    "${FUNC_GATE_SCOPE}[^ ]*:[0-9]+:[0-9]+: (warning: .*readability-function-(size|cognitive-complexity)|note: [0-9]+ lines including)")

# FUNC_GATE_TIDY_LOG replays a recorded clang-tidy output so the gate's parsing can be tested against fixed
# diagnostics; FUNC_GATE_TIDY_RC with FUNC_GATE_TIDY_ERR adds the run's exit status and error output for the run check.
set(_tidy_log "${FUNC_GATE_TIDY_LOG}")
if(FUNC_GATE_TIDY_LOG AND DEFINED FUNC_GATE_TIDY_RC)
    file(READ "${FUNC_GATE_TIDY_ERR}" _tidy_err)
    function_size_gate_check_run("${_tidy_log}" "${FUNC_GATE_TIDY_RC}" "${_tidy_err}")
elseif(NOT FUNC_GATE_TIDY_LOG)
    set(_tidy_log "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy.log")
    find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
    if(NOT RUN_CLANG_TIDY)
        message(WARNING
            "function size gate: run-clang-tidy not found -- the function-size + "
            "cognitive-complexity gate is NOT active in this tree. Install LLVM's "
            "run-clang-tidy to enforce the function ceiling at build time.")
        return()
    endif()

    # -checks keeps an unrelated diagnostic out of the result. clang-tidy counts a body from brace to brace, so a zero
    # line threshold reports every body's length for the screen count; -config replaces .clang-tidy, hence the
    # restated limits. The log keeps only the lines the gate reads, each once: a header's diagnostics repeat in every TU
    # that includes it, which made the raw output hundreds of megabytes. The filters run in the C locale so a source
    # line's non-UTF-8 byte cannot make grep treat the stream as binary.
    string(CONCAT _tidy_config "{CheckOptions: {"
        "readability-function-size.LineThreshold: 0, readability-function-size.StatementThreshold: -1, "
        "readability-function-cognitive-complexity.Threshold: 25, readability-function-cognitive-complexity.IgnoreMacros: true}}")
    execute_process(
        COMMAND ${RUN_CLANG_TIDY}
                -p ${FUNC_GATE_BUILD_DIR}
                -quiet
                -checks=-*,readability-function-size,readability-function-cognitive-complexity
                "-config=${_tidy_config}"
                "-header-filter=.*/(lib|backends)/plexus.*"
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C grep -a -E "${_diagnostic_regex}|terminated by signal|^Error while processing "
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C awk "!seen[$0]++"
        OUTPUT_FILE ${_tidy_log}
        ERROR_VARIABLE _tidy_err
        RESULTS_VARIABLE _tidy_results)
    list(GET _tidy_results 0 _tidy_rc)
    function_size_gate_check_run("${_tidy_log}" "${_tidy_rc}" "${_tidy_err}")
endif()

# Read line by line, so a quoted source line's unbalanced bracket cannot fold neighboring diagnostics together.
file(STRINGS "${_tidy_log}" _diagnostics REGEX "${_diagnostic_regex}")
list(REMOVE_DUPLICATES _diagnostics)
function_size_gate_overages(_diagnostics _violations)
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
