# Checks the CONVENTIONS.md include order in every C++ source and header under lib, backends, tests, examples, tools
# and benchmarks; a root holding none of those directories, such as a negative-fixture directory, is scanned whole. A
# quoted include is project when it names a scanned file, and an angle include is standard when it names a C++
# standard header. Included from the build, this file registers an ALL target that re-runs it through -P with
# INCLUDE_GATE_FATAL=ON, so any violation fails the build; a -P run without INCLUDE_GATE_FATAL only warns.

if(NOT INCLUDE_GATE_SCAN)
    add_custom_target(include_order_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DINCLUDE_GATE_SCAN=ON
                -DINCLUDE_GATE_FATAL=ON
                -DINCLUDE_GATE_ROOT=${PROJECT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Checking the include order of every C++ file"
        VERBATIM)
    return()
endif()

# A bare -P run sets no policy, and the line lists need CMP0007's empty elements.
cmake_policy(VERSION 3.25)
include(${CMAKE_CURRENT_LIST_DIR}/include_order_rules.cmake)

set(_scan_roots "")
set(_root_names "")
foreach(_sub IN ITEMS lib backends tests examples tools benchmarks)
    if(IS_DIRECTORY "${INCLUDE_GATE_ROOT}/${_sub}")
        list(APPEND _scan_roots "${INCLUDE_GATE_ROOT}/${_sub}")
        list(APPEND _root_names ${_sub})
    endif()
endforeach()
if(NOT _scan_roots)
    set(_scan_roots "${INCLUDE_GATE_ROOT}")
endif()

set(_globs "")
foreach(_root IN LISTS _scan_roots)
    foreach(_pattern IN ITEMS *.h *.hpp *.cpp *.cc)
        list(APPEND _globs "${_root}/${_pattern}")
    endforeach()
endforeach()
file(GLOB_RECURSE _scan_files ${_globs})

string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" _root_re "${INCLUDE_GATE_ROOT}")
list(FILTER _scan_files EXCLUDE REGEX "^${_root_re}/tests/include_order_gate/")
list(FILTER _scan_files EXCLUDE REGEX "/(cmake-)?build([-_][^/]*)?/")

# A file git ignores, such as a local credentials header, is not part of the repository; counting it as project would
# make the result depend on the checkout. Without git the glob stands as it is.
find_program(_inc_git git)
if(_inc_git)
    execute_process(COMMAND "${_inc_git}" ls-files -o -i --exclude-standard --directory -- ${_root_names}
        WORKING_DIRECTORY "${INCLUDE_GATE_ROOT}" OUTPUT_VARIABLE _ignored RESULT_VARIABLE _git_rc ERROR_QUIET)
    if(_git_rc EQUAL 0)
        string(REPLACE "\n" ";" _ignored "${_ignored}")
        foreach(_entry IN LISTS _ignored)
            if(NOT _entry STREQUAL "")
                string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" _entry_re "${_entry}")
                string(REGEX REPLACE "([^/])$" "\\1$" _entry_re "${_entry_re}")
                list(FILTER _scan_files EXCLUDE REGEX "^${_root_re}/${_entry_re}")
            endif()
        endforeach()
    endif()
endif()

foreach(_file IN LISTS _scan_files)
    file(RELATIVE_PATH _rel "${INCLUDE_GATE_ROOT}" "${_file}")
    set(_inc_file_${_rel} 1)
    string(REPLACE "/" ";" _parts "${_rel}")
    while(NOT _parts STREQUAL "")
        list(JOIN _parts "/" _suffix)
        set(_inc_proj_${_suffix} 1)
        list(POP_FRONT _parts)
    endwhile()
endforeach()

macro(_inc_dot var)
    if(${var} STREQUAL "")
        set(${var} .)
    endif()
endmacro()

# A header below include/ or shm-include/ owns its directory there; another file owns the directory of the quoted
# include named after it, or its own directory when it quotes a file beside it; otherwise it has none ("").
function(_inc_own_dir rel quoted out_var)
    set(_own "")
    get_filename_component(_stem "${rel}" NAME_WLE)
    get_filename_component(_here "${rel}" DIRECTORY)
    foreach(_path IN LISTS quoted)
        get_filename_component(_name "${_path}" NAME_WLE)
        if(_own STREQUAL "" AND _name STREQUAL _stem)
            get_filename_component(_own "${_path}" DIRECTORY)
            _inc_dot(_own)
        endif()
    endforeach()
    foreach(_path IN LISTS quoted)
        string(REGEX REPLACE "^/" "" _beside "${_here}/${_path}")
        if(_own STREQUAL "" AND NOT _path MATCHES "/" AND DEFINED _inc_file_${_beside})
            set(_own .)
        endif()
    endforeach()
    if(rel MATCHES "(^|/)(include|shm-include)/(.+)$")
        get_filename_component(_own "${CMAKE_MATCH_3}" DIRECTORY)
        _inc_dot(_own)
    endif()
    set(${out_var} "${_own}" PARENT_SCOPE)
endfunction()

# Before the text becomes a list of lines, ';' would split a line and '[', ']' and '\' would nest or escape list
# elements; none can occur in an include path, so each is replaced.
function(_inc_scan_file file)
    file(READ "${file}" _text)
    if(NOT _text MATCHES "#[ \t]*include")
        return()
    endif()
    file(RELATIVE_PATH _inc_rel "${INCLUDE_GATE_ROOT}" "${file}")
    string(REGEX MATCHALL "(^|\n)[ \t]*#[ \t]*include[ \t]*\"[^\"\n]+\"" _quoted "${_text}")
    list(TRANSFORM _quoted REPLACE "^[^\"]*\"([^\"]+)\"$" "\\1")
    _inc_own_dir("${_inc_rel}" "${_quoted}" _inc_own)
    foreach(_char IN ITEMS "[" "]" "\\")
        string(REPLACE "${_char}" "_" _text "${_text}")
    endforeach()
    string(REPLACE ";" "_" _text "${_text}")
    string(REPLACE "\n" ";" _lines "${_text}")
    set(_ln 0)
    set(_block "")
    foreach(_line IN LISTS _lines)
        math(EXPR _ln "${_ln} + 1")
        _inc_classify_line()
    endforeach()
    if(NOT _block STREQUAL "")
        _inc_check_block(${_block})
    endif()
endfunction()

macro(_inc_classify_line)
    if(_line MATCHES "^[ \t]*#[ \t]*include[ \t]*([<\"])([^>\"]+)[>\"]")
        string(REPLACE "<" "A" _quote "${CMAKE_MATCH_1}")
        string(REPLACE "\"" "Q" _quote "${_quote}")
        list(APPEND _block "${_ln}|${_quote}|${CMAKE_MATCH_2}")
    elseif(_line MATCHES "^[ \t\r]*$")
        if(NOT _block STREQUAL "")
            list(APPEND _block "${_ln}|B|")
        endif()
    elseif(NOT _block STREQUAL "")
        _inc_check_block(${_block})
        set(_block "")
    endif()
endmacro()

foreach(_file IN LISTS _scan_files)
    _inc_scan_file("${_file}")
endforeach()

list(LENGTH _scan_files _scanned)
get_property(_hits GLOBAL PROPERTY INCLUDE_GATE_HITS)
list(LENGTH _hits _nviol)
if(_nviol EQUAL 0)
    message(STATUS "include order gate: clean (${_scanned} files scanned)")
    return()
endif()
list(REMOVE_DUPLICATES _hits)
list(LENGTH _hits _nfiles)
set(_summary "include order gate: ${_nviol} violation(s) in ${_nfiles} file(s) (${_scanned} files scanned)")
if(INCLUDE_GATE_FATAL)
    message(FATAL_ERROR "${_summary}")
else()
    message(WARNING "${_summary}\n(advisory: not yet build-failing)")
endif()
