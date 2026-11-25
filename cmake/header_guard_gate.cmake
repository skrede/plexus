# Fails the build on a header under lib, backends, tools, tests, examples or benchmarks, or a .h.example template, whose
# guard does not follow its path, and on two headers that derive one guard; a root holding none of those directories,
# such as a fixture directory, is scanned whole. The scan is git's list of tracked files rather than a glob, so a
# gitignored header (a local credentials copy, an in-tree build) is never read and a new header counts once staged.
# Included from the build, this file registers an ALL target that re-runs it through -P with HEADER_GATE_FATAL=ON; a
# -P run without HEADER_GATE_FATAL only warns.

if(NOT HEADER_GATE_SCAN)
    add_custom_target(header_guard_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DHEADER_GATE_SCAN=ON
                -DHEADER_GATE_FATAL=ON
                -DHEADER_GATE_ROOT=${PROJECT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Checking every header's guard against its path"
        VERBATIM)
    return()
endif()

# A bare -P run sets no policy.
cmake_policy(VERSION 3.25)

function(_guard_hit rel kind detail)
    message(STATUS "header guard gate: ${rel}: ${kind}: ${detail}")
    set_property(GLOBAL APPEND PROPERTY HEADER_GATE_HITS "${rel}")
endfunction()

# Without git, or outside a work tree, there is no list of tracked files: the gate checks nothing and says why.
function(_guard_find_git out_var)
    set(${out_var} "" PARENT_SCOPE)
    find_program(_git git)
    if(NOT _git)
        message(WARNING "header guard gate: git not found -- no header was checked")
        return()
    endif()
    execute_process(COMMAND "${_git}" rev-parse --is-inside-work-tree WORKING_DIRECTORY "${HEADER_GATE_ROOT}"
        OUTPUT_VARIABLE _inside ERROR_VARIABLE _why RESULT_VARIABLE _rc OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _rc EQUAL 0 OR NOT _inside STREQUAL "true")
        string(STRIP "${_why}" _why)
        message(WARNING "header guard gate: ${HEADER_GATE_ROOT} is not inside a git work tree (${_why}) -- no header was checked")
        return()
    endif()
    set(${out_var} "${_git}" PARENT_SCOPE)
endfunction()

function(_guard_tracked_headers git out_var)
    set(_specs "")
    foreach(_sub IN ITEMS lib backends tools tests examples benchmarks)
        if(IS_DIRECTORY "${HEADER_GATE_ROOT}/${_sub}")
            list(APPEND _specs "${_sub}/*.h" "${_sub}/*.hpp" "${_sub}/*.h.example")
        endif()
    endforeach()
    if(NOT _specs)
        set(_specs "*.h" "*.hpp" "*.h.example")
    endif()
    execute_process(COMMAND "${git}" -c core.quotePath=false ls-files -- ${_specs} ":(exclude)tests/header_guard_gate"
        WORKING_DIRECTORY "${HEADER_GATE_ROOT}" OUTPUT_VARIABLE _listed ERROR_VARIABLE _why RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "header guard gate: git ls-files failed (exit ${_rc}): ${_why}")
    endif()
    string(REGEX REPLACE "\n$" "" _listed "${_listed}")
    string(REPLACE "\n" ";" _listed "${_listed}")
    set(${out_var} "${_listed}" PARENT_SCOPE)
endfunction()

# A library or tool header, and an example header below an include directory, is named from below its last include or
# shm-include directory, as an include line names it; every other header is named from the root.
function(_guard_derive rel out_var)
    string(REGEX REPLACE "\\.(h\\.example|hpp|h)$" "" _name "${rel}")
    if(_name MATCHES "^(lib|backends|tools|examples)/" AND _name MATCHES "^(.*/)?(include|shm-include)/(.+)$")
        set(_name "${CMAKE_MATCH_3}")
    endif()
    string(REGEX REPLACE "[^A-Za-z0-9]" "_" _name "${_name}")
    string(TOUPPER "${_name}" _name)
    set(${out_var} "HPP_GUARD_${_name}_H" PARENT_SCOPE)
endfunction()

# The guard is the header's first two preprocessor lines, indented or not, since clang-format indents a #define that
# does not match its #ifndef. Carriage returns are dropped first, so a CRLF checkout reads as the repository does.
function(_guard_check_file rel derived)
    file(READ "${HEADER_GATE_ROOT}/${rel}" _text)
    string(REPLACE "\r" "" _text "${_text}")
    if(_text MATCHES "(^|\n)[ \t]*#[ \t]*pragma[ \t]+once")
        _guard_hit("${rel}" pragma "#pragma once instead of the guard ${derived}")
        return()
    endif()
    set(_other "([ \t]*([^# \t\n][^\n]*)?\n)*")
    if(NOT _text MATCHES "^${_other}[ \t]*#ifndef ([^ \t\n]+)\n${_other}[ \t]*#define ([^ \t\n]+)\n")
        _guard_hit("${rel}" missing "the first two preprocessor lines are not #ifndef and #define ${derived}")
        return()
    endif()
    if(NOT CMAKE_MATCH_6 STREQUAL CMAKE_MATCH_3)
        _guard_hit("${rel}" define "#define ${CMAKE_MATCH_6} differs from #ifndef ${CMAKE_MATCH_3}")
    endif()
    if(NOT CMAKE_MATCH_3 STREQUAL derived)
        _guard_hit("${rel}" path "${CMAKE_MATCH_3}, derived ${derived}")
    endif()
    string(STRIP "${_text}" _text)
    string(REGEX MATCH "[^\n]*$" _last "${_text}")
    if(NOT _last STREQUAL "#endif")
        _guard_hit("${rel}" endif "the last line is '${_last}', not a bare #endif")
    endif()
endfunction()

_guard_find_git(_git)
set(_headers "")
if(_git)
    _guard_tracked_headers("${_git}" _headers)
endif()
set(_checked 0)
foreach(_rel IN LISTS _headers)
    if(NOT EXISTS "${HEADER_GATE_ROOT}/${_rel}")
        continue()
    endif()
    math(EXPR _checked "${_checked} + 1")
    _guard_derive("${_rel}" _derived)
    if(DEFINED _guard_owner_${_derived})
        _guard_hit("${_rel}" collision "${_derived}, also derived by ${_guard_owner_${_derived}}")
    else()
        set(_guard_owner_${_derived} "${_rel}")
    endif()
    _guard_check_file("${_rel}" "${_derived}")
endforeach()

get_property(_hits GLOBAL PROPERTY HEADER_GATE_HITS)
list(LENGTH _hits _nviol)
if(_nviol EQUAL 0)
    message(STATUS "header guard gate: clean (${_checked} headers checked)")
    return()
endif()
list(REMOVE_DUPLICATES _hits)
list(LENGTH _hits _nfiles)
set(_summary "header guard gate: ${_nviol} violation(s) in ${_nfiles} file(s) (${_checked} headers checked)")
if(HEADER_GATE_FATAL)
    message(FATAL_ERROR "${_summary}")
else()
    message(WARNING "${_summary}")
endif()
