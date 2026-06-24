# The size assertion: no source unit may exceed the conventions.md file ceiling
# (200 lines, comments and blank lines included) unless it is a registered
# exception. The scan set is the whole tree -- lib/, backends/, tests/, examples/
# -- minus the gate's own negative-fixture dir. An over-limit file is legal only when it
# carries a matching row in EXCEPTIONS.md; a row for a unit that is no longer
# over-limit, or that names no scanned file, is itself a violation (stale exceptions
# are not silently tolerated). This file is self-dispatching: included from the
# build it registers an ALL custom target that re-invokes it via -P
# (cross-platform, no bash) with SIZE_GATE_FATAL=ON, so a violation fails the
# build; run with -DSIZE_GATE_SCAN it performs the scan. A direct -P invocation
# without -DSIZE_GATE_FATAL=ON stays advisory (WARNING per violation) for
# probing the tree by hand.

set(SIZE_GATE_LIMIT 200)

if(NOT SIZE_GATE_SCAN)
    add_custom_target(size_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DSIZE_GATE_SCAN=ON
                -DSIZE_GATE_FATAL=ON
                -DSIZE_GATE_ROOT=${PROJECT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting source units stay within the file-size ceiling"
        VERBATIM)
    return()
endif()

# Scan the conventional tree-wide scope when it exists; otherwise scan the root
# directly (the targeted negative-fixture invocation passes a leaf dir).
set(_scan_roots "")
foreach(_sub IN ITEMS lib backends tests examples)
    if(IS_DIRECTORY "${SIZE_GATE_ROOT}/${_sub}")
        list(APPEND _scan_roots "${SIZE_GATE_ROOT}/${_sub}")
    endif()
endforeach()
if(NOT _scan_roots)
    set(_scan_roots "${SIZE_GATE_ROOT}")
endif()

set(_globs "")
foreach(_root IN LISTS _scan_roots)
    list(APPEND _globs
        "${_root}/*.h" "${_root}/*.hpp" "${_root}/*.cpp" "${_root}/*.cc")
endforeach()
file(GLOB_RECURSE _scan_files ${_globs})

# The gate's own deliberately-malformed fixtures live here and must never
# pollute the tree-wide result; they are scanned only by a targeted invocation
# that roots SIZE_GATE_ROOT at the fixture dir itself.
set(_fixture_prefix "${SIZE_GATE_ROOT}/tests/size_gate/")
string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" _fixture_re "${_fixture_prefix}")
list(FILTER _scan_files EXCLUDE REGEX "^${_fixture_re}")

# Generated build trees (e.g. an in-tree ESP-IDF cross-build under examples/) are
# machine output, not hand-written source -- never gate them. Drop any path whose
# component is a build/, build-<suffix>/, or cmake-build-<suffix>/ artifact dir,
# matching the .gitignore'd patterns (the JetBrains cmake-build-* layout included).
list(FILTER _scan_files EXCLUDE REGEX "/(cmake-)?build([-_][^/]*)?/")

# Parse EXCEPTIONS.md once into the set of registered file paths (the first
# pipe-delimited cell of each data row that names an existing source file).
set(_registered "")
set(_exceptions_md "${SIZE_GATE_ROOT}/EXCEPTIONS.md")
if(EXISTS "${_exceptions_md}")
    file(STRINGS "${_exceptions_md}" _md_lines)
    foreach(_md IN LISTS _md_lines)
        if(_md MATCHES "^\\|[ \t]*([^|]+[^| \t])[ \t]*\\|")
            set(_cell "${CMAKE_MATCH_1}")
            string(STRIP "${_cell}" _cell)
            if(_cell MATCHES "\\.(h|hpp|cpp|cc)$")
                list(APPEND _registered "${_cell}")
            endif()
        endif()
    endforeach()
endif()

# Count lines exactly as `wc -l` does: the number of newline terminators. We
# read the raw bytes and take the length delta after stripping newlines, which
# is immune to the embedded-semicolon corruption that makes file(STRINGS)'s list
# length over-count (CMake splits a list on both '\n' AND ';', so a ';' inside a
# C++ source line spuriously inflates the count). A trailing line with no final
# newline is not counted, matching `wc -l`.
function(_size_line_count file out_var)
    file(READ "${file}" _content)
    string(LENGTH "${_content}" _with)
    string(REPLACE "\n" "" _content "${_content}")
    string(LENGTH "${_content}" _without)
    math(EXPR _n "${_with} - ${_without}")
    set(${out_var} ${_n} PARENT_SCOPE)
endfunction()

set(_violations "")
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
        endif()
    else()
        if(_has_row)
            list(APPEND _violations
                "${_rel}: has an EXCEPTIONS.md row but is only ${_count} lines (stale exception)")
        endif()
    endif()
endforeach()

# A registered row that names no scanned file at all is a stale exception too.
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
