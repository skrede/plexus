# Fails the build on any code file over the CONVENTIONS.md file ceiling (200 lines, comments and blank lines
# included) that EXCEPTIONS.md does not sanction. The code files are C++ sources and headers, CMakeLists.txt, *.cmake,
# *.sh, *.py and *.lua under lib, backends, tests, examples, cmake, tools and benchmarks, plus the root CMakeLists.txt;
# a root holding none of those directories, such as a negative-fixture directory, is scanned whole. A registered file
# may shrink freely but never grow past its row's ceiling, and a row that is stale, duplicated, of another kind or
# without a valid ceiling is itself a violation.
# Included from the build, this file registers an ALL target that re-runs it through -P with SIZE_GATE_FATAL=ON, so the
# check needs no shell and runs the same on Windows; a -P run without SIZE_GATE_FATAL only warns.

set(SIZE_GATE_LIMIT 200)

if(NOT SIZE_GATE_SCAN)
    add_custom_target(size_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DSIZE_GATE_SCAN=ON
                -DSIZE_GATE_FATAL=ON
                -DSIZE_GATE_ROOT=${PROJECT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting code files stay within the file-size ceiling"
        VERBATIM)
    return()
endif()

set(_patterns *.h *.hpp *.cpp *.cc CMakeLists.txt *.cmake *.sh *.py *.lua)
set(_scan_roots "")
foreach(_sub IN ITEMS lib backends tests examples cmake tools benchmarks)
    if(IS_DIRECTORY "${SIZE_GATE_ROOT}/${_sub}")
        list(APPEND _scan_roots "${SIZE_GATE_ROOT}/${_sub}")
    endif()
endforeach()
set(_scan_files "")
if(_scan_roots AND EXISTS "${SIZE_GATE_ROOT}/CMakeLists.txt")
    list(APPEND _scan_files "${SIZE_GATE_ROOT}/CMakeLists.txt")
endif()
if(NOT _scan_roots)
    set(_scan_roots "${SIZE_GATE_ROOT}")
endif()

set(_globs "")
foreach(_root IN LISTS _scan_roots)
    foreach(_pattern IN LISTS _patterns)
        list(APPEND _globs "${_root}/${_pattern}")
    endforeach()
endforeach()
file(GLOB_RECURSE _globbed ${_globs})
list(APPEND _scan_files ${_globbed})
list(REMOVE_DUPLICATES _scan_files)

# The gate's own over-limit fixtures would fail every tree-wide run; only a run rooted at their directory scans them.
set(_fixture_prefix "${SIZE_GATE_ROOT}/tests/size_gate/")
string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" _fixture_re "${_fixture_prefix}")
list(FILTER _scan_files EXCLUDE REGEX "^${_fixture_re}")

# Build trees (an in-tree ESP-IDF build under examples/, FetchContent checkouts) are generated, not written. The
# pattern matches the gitignored build/, build-*/ and cmake-build-*/ path components.
list(FILTER _scan_files EXCLUDE REGEX "/(cmake-)?build([-_][^/]*)?/")

# EXCEPTIONS.md becomes two index-aligned lists, paths and ceilings, since CMake has no map. A file row's first cell
# names a code file; a function row's starts with a backtick and never matches. A path row of another kind, or a second
# row for one path, is a violation rather than a registration, so no row can shadow or stand in for another.
set(_registered "")
set(_ceilings "")
set(_violations "")
set(_exceptions_md "${SIZE_GATE_ROOT}/EXCEPTIONS.md")
if(EXISTS "${_exceptions_md}")
    file(STRINGS "${_exceptions_md}" _md_lines)
    foreach(_md IN LISTS _md_lines)
        if(NOT _md MATCHES "^\\|([^|]+)\\|([^|]+)\\|([^|]+)\\|")
            continue()
        endif()
        string(STRIP "${CMAKE_MATCH_1}" _cell)
        string(STRIP "${CMAKE_MATCH_2}" _kind)
        string(STRIP "${CMAKE_MATCH_3}" _ceiling)
        if(NOT _cell MATCHES "(\\.(h|hpp|cpp|cc|cmake|sh|py|lua)|(^|/)CMakeLists\\.txt)$")
            continue()
        endif()
        if(NOT _kind STREQUAL "file")
            list(APPEND _violations "${_cell}: EXCEPTIONS.md row kind '${_kind}' is not file")
            continue()
        endif()
        list(FIND _registered "${_cell}" _dup_idx)
        if(NOT _dup_idx EQUAL -1)
            list(APPEND _violations "${_cell}: duplicate EXCEPTIONS.md row")
            continue()
        endif()
        list(APPEND _registered "${_cell}")
        # An invalid cell stores a non-numeric sentinel so a blank or ';'-bearing cell cannot shift the alignment.
        if(_ceiling MATCHES "^[0-9]+$" AND _ceiling GREATER SIZE_GATE_LIMIT)
            list(APPEND _ceilings "${_ceiling}")
        else()
            list(APPEND _ceilings "-")
            list(APPEND _violations
                "${_cell}: EXCEPTIONS.md row has no valid ceiling above ${SIZE_GATE_LIMIT}")
        endif()
    endforeach()
endif()

# Counts newline terminators, as `wc -l` does, from the raw bytes: file(STRINGS) splits its list on ';' as well as on
# newlines, so a semicolon inside a line would inflate the count. A last line without a newline is not counted.
function(_size_line_count file out_var)
    file(READ "${file}" _content)
    string(LENGTH "${_content}" _with)
    string(REPLACE "\n" "" _content "${_content}")
    string(LENGTH "${_content}" _without)
    math(EXPR _n "${_with} - ${_without}")
    set(${out_var} ${_n} PARENT_SCOPE)
endfunction()

set(_seen_registered "")

foreach(_file IN LISTS _scan_files)
    _size_line_count("${_file}" _count)
    file(RELATIVE_PATH _rel "${SIZE_GATE_ROOT}" "${_file}")

    list(FIND _registered "${_rel}" _row_idx)
    set(_has_row FALSE)
    if(NOT _row_idx EQUAL -1)
        set(_has_row TRUE)
        list(APPEND _seen_registered "${_rel}")
    endif()

    if(_count GREATER SIZE_GATE_LIMIT)
        if(NOT _has_row)
            list(APPEND _violations
                "${_rel}: ${_count} lines over the ${SIZE_GATE_LIMIT} ceiling with no EXCEPTIONS.md row")
        else()
            list(GET _ceilings ${_row_idx} _ceiling)
            if(_ceiling MATCHES "^[0-9]+$" AND _count GREATER _ceiling)
                list(APPEND _violations
                    "${_rel}: ${_count} lines over its registered ceiling ${_ceiling}")
            endif()
        endif()
    else()
        if(_has_row)
            list(APPEND _violations
                "${_rel}: has an EXCEPTIONS.md row but is only ${_count} lines (stale exception)")
        endif()
    endif()
endforeach()

foreach(_reg IN LISTS _registered)
    list(FIND _seen_registered "${_reg}" _seen_idx)
    if(_seen_idx EQUAL -1)
        list(APPEND _violations
            "${_reg}: EXCEPTIONS.md row names a file not found in the scan (stale exception)")
    endif()
endforeach()

list(LENGTH _scan_files _scanned)
list(LENGTH _violations _nviol)

if(_nviol EQUAL 0)
    message(STATUS "size gate: clean (${_scanned} files scanned, ${SIZE_GATE_LIMIT}-line ceiling)")
    return()
endif()

set(_report "size gate: ${_nviol} violation(s):")
foreach(_v IN LISTS _violations)
    string(APPEND _report "\n  - ${_v}")
endforeach()

if(SIZE_GATE_FATAL)
    message(FATAL_ERROR "${_report}")
else()
    message(WARNING "${_report}\n(advisory: not yet build-failing)")
endif()
