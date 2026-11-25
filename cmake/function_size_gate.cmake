# No library function may exceed the cognitive-complexity ceiling, nor the function ceiling (25 lines as it sits on
# screen, from its signature's first line to its closing brace, comments and blanks included) unless an EXCEPTIONS.md
# function row names its file and function, up to the row's ceiling. clang-tidy checks a header's bodies through any
# TU that includes it, and lib/ is header-only, so the run visits the whole compile database, test TUs included, and
# -header-filter scopes diagnostics back to the library. clang-tidy runs advisory and the gate fails itself on a
# surviving library diagnostic, because -warnings-as-errors keys on the check, not its location, and -header-filter
# cannot suppress a TU's own main file. Included from the build, the file registers an ALL target that re-runs it
# through -P with FUNC_GATE_FATAL=ON; a bare -P run stays advisory.
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
                -DFUNC_GATE_ROOT=${PROJECT_SOURCE_DIR}
                -DFUNC_GATE_EXCEPTIONS=${PROJECT_SOURCE_DIR}/EXCEPTIONS.md
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting library functions stay within the size + cognitive-complexity ceilings"
        VERBATIM)
    return()
endif()

include(${CMAKE_CURRENT_LIST_DIR}/function_size_gate_lines.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/function_size_gate_run.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/function_size_gate_exceptions.cmake)

function(function_size_gate_report_section title items report)
    if(NOT ${report} STREQUAL "")
        string(APPEND ${report} "\n")
    endif()
    string(APPEND ${report} "function size gate: ${title}:")
    foreach(_v IN LISTS ${items})
        string(STRIP "${_v}" _v)
        string(APPEND ${report} "\n  - ${_v}")
    endforeach()
    set(${report} "${${report}}" PARENT_SCOPE)
endfunction()

if(FUNC_GATE_SOURCES_FILE)
    function_size_gate_check_coverage()
endif()

# A replay without FUNC_GATE_EXCEPTIONS reads no rows; one naming a missing file, or rows without a root to resolve
# their files against, fails before the run.
if(FUNC_GATE_EXCEPTIONS)
    if(NOT EXISTS "${FUNC_GATE_EXCEPTIONS}" OR NOT IS_DIRECTORY "${FUNC_GATE_ROOT}")
        message(FATAL_ERROR "function size gate: no exceptions file '${FUNC_GATE_EXCEPTIONS}' or root '${FUNC_GATE_ROOT}'")
    endif()
    function_size_gate_read_rows("${FUNC_GATE_EXCEPTIONS}")
endif()

set(_diagnostic_regex
    "${FUNC_GATE_SCOPE}[^ ]*:[0-9]+:[0-9]+: (warning: .*readability-function-(size|cognitive-complexity)|note: [0-9]+ lines including)")

# FUNC_GATE_TIDY_LOG replays a recorded clang-tidy output so the gate's parsing can be tested against fixed
# diagnostics; FUNC_GATE_TIDY_RC with FUNC_GATE_TIDY_ERR adds the run's exit status and error output for the run check.
set(_tidy_log "${FUNC_GATE_TIDY_LOG}")
if(FUNC_GATE_TIDY_LOG AND DEFINED FUNC_GATE_TIDY_RC)
    function_size_gate_check_run("${_tidy_log}" "${FUNC_GATE_TIDY_RC}" "${FUNC_GATE_TIDY_ERR}")
elseif(NOT FUNC_GATE_TIDY_LOG)
    set(_tidy_log "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy.log")
    set(_tidy_err "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy.err")
    find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
    if(NOT RUN_CLANG_TIDY)
        message(WARNING
            "function size gate: run-clang-tidy not found -- the function-size + "
            "cognitive-complexity gate is NOT active in this tree. Install LLVM's "
            "run-clang-tidy to enforce the function ceiling at build time.")
        return()
    endif()

    function_size_gate_tidy(function_size_gate_tidy _tidy_rc)
    function_size_gate_rerun_crashed(_tidy_rc _tidy_log _tidy_err)
    function_size_gate_check_run("${_tidy_log}" "${_tidy_rc}" "${_tidy_err}")
endif()

# Read line by line, so a quoted source line's unbalanced bracket cannot fold neighboring diagnostics together.
file(STRINGS "${_tidy_log}" _diagnostics REGEX "${_diagnostic_regex}")
list(REMOVE_DUPLICATES _diagnostics)
function_size_gate_overages(_diagnostics _violations)
function_size_gate_row_verdicts(_violations _row_violations)
list(LENGTH _violations _nviol)
list(LENGTH _row_violations _nrows)

if(_nviol EQUAL 0 AND _nrows EQUAL 0)
    message(STATUS "function size gate: clean (no unsuppressed library function overage)")
    return()
endif()

set(_report "")
if(_nviol GREATER 0)
    function_size_gate_report_section("${_nviol} unsuppressed library function overage(s)" _violations _report)
endif()
if(_nrows GREATER 0)
    function_size_gate_report_section("${_nrows} function exception row violation(s)" _row_violations _report)
endif()
string(APPEND _report
    "\n(register an earned overage as a function row in EXCEPTIONS.md, naming its file and function; "
    "never loosen the ceiling)")

if(FUNC_GATE_FATAL)
    message(FATAL_ERROR "${_report}")
else()
    message(WARNING "${_report}\n(advisory: not yet build-failing)")
endif()
