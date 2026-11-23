# No library function may exceed the function ceiling (25 lines, comments and blanks included) or the
# cognitive-complexity ceiling unless it carries a function-scoped NOLINTNEXTLINE(readability-function-size) and an
# EXCEPTIONS.md row. clang-tidy checks a header's bodies through any TU that includes it, and lib/ is header-only, so
# the run visits the whole compile database, test TUs included, and -header-filter scopes diagnostics back to the
# library. A source compiled by a lib/ or backends/ target but absent from the database would go unchecked, so the
# gate fails on one. clang-tidy runs advisory and the gate fails itself on a surviving library diagnostic, because
# -warnings-as-errors keys on the check, not its location, and -header-filter cannot suppress a TU's own main file.
# Included from the build, the file registers an ALL target that re-runs it through -P with FUNC_GATE_FATAL=ON; a
# bare -P run stays advisory.
set(FUNC_GATE_SCOPE "/(lib|backends)/plexus")

function(function_size_gate_target_sources target out)
    set(_found "")
    get_target_property(_type ${target} TYPE)
    if(NOT _type MATCHES "^(STATIC_LIBRARY|SHARED_LIBRARY|MODULE_LIBRARY|OBJECT_LIBRARY|EXECUTABLE)$")
        set(${out} "" PARENT_SCOPE)
        return()
    endif()
    get_target_property(_sources ${target} SOURCES)
    get_target_property(_source_dir ${target} SOURCE_DIR)
    foreach(_source IN LISTS _sources)
        if(_source MATCHES "\\.(cpp|cc)$" AND NOT _source MATCHES "^\\$<")
            cmake_path(ABSOLUTE_PATH _source BASE_DIRECTORY "${_source_dir}" NORMALIZE OUTPUT_VARIABLE _absolute)
            list(APPEND _found "${_absolute}")
        endif()
    endforeach()
    set(${out} "${_found}" PARENT_SCOPE)
endfunction()

function(function_size_gate_directory_sources dir out)
    set(_found "")
    get_property(_targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_target IN LISTS _targets)
        function_size_gate_target_sources(${_target} _target_sources)
        list(APPEND _found ${_target_sources})
    endforeach()
    get_property(_subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
    foreach(_subdir IN LISTS _subdirs)
        function_size_gate_directory_sources("${_subdir}" _subdir_sources)
        list(APPEND _found ${_subdir_sources})
    endforeach()
    set(${out} "${_found}" PARENT_SCOPE)
endfunction()

# The directory walk, not a target list, so a compiled backend added later is covered without editing the gate.
function(function_size_gate_library_sources out)
    set(_found "")
    get_property(_subdirs DIRECTORY "${PROJECT_SOURCE_DIR}" PROPERTY SUBDIRECTORIES)
    foreach(_subdir IN LISTS _subdirs)
        file(RELATIVE_PATH _relative "${PROJECT_SOURCE_DIR}" "${_subdir}")
        if(_relative MATCHES "^(lib|backends)/")
            function_size_gate_directory_sources("${_subdir}" _subdir_sources)
            list(APPEND _found ${_subdir_sources})
        endif()
    endforeach()
    set(${out} "${_found}" PARENT_SCOPE)
endfunction()

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

# One regex over the whole database, because a per-entry string(JSON ... GET) re-parses the file on every call.
function(function_size_gate_database_files out)
    set(_database "")
    if(EXISTS "${FUNC_GATE_BUILD_DIR}/compile_commands.json")
        file(READ "${FUNC_GATE_BUILD_DIR}/compile_commands.json" _database)
    endif()
    string(REGEX MATCHALL "\"file\"[ \t]*:[ \t]*\"[^\"]*\"" _entries "${_database}")
    set(_files "")
    foreach(_entry IN LISTS _entries)
        string(REGEX REPLACE "^\"file\"[ \t]*:[ \t]*\"(.*)\"$" "\\1" _file "${_entry}")
        cmake_path(NORMAL_PATH _file)
        list(APPEND _files "${_file}")
    endforeach()
    set(${out} "${_files}" PARENT_SCOPE)
endfunction()

function(function_size_gate_check_coverage)
    file(STRINGS "${FUNC_GATE_SOURCES_FILE}" _sources)
    function_size_gate_database_files(_files)
    set(_report "")
    foreach(_source IN LISTS _sources)
        list(FIND _files "${_source}" _at)
        if(_at EQUAL -1)
            string(APPEND _report "\n  function size gate: ${_source} is compiled by a library target but missing from the compile database")
        endif()
    endforeach()
    if(_report STREQUAL "")
        return()
    endif()
    if(FUNC_GATE_FATAL)
        message(FATAL_ERROR "function size gate: the compile database misses a library source:${_report}")
    endif()
    message(WARNING "function size gate: the compile database misses a library source:${_report}")
endfunction()

if(FUNC_GATE_SOURCES_FILE)
    function_size_gate_check_coverage()
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
