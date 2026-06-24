# The seam assertion: no Linux shared-memory primitive may appear in the generic
# library tree. The scan set is the WHOLE lib/ tree -- the POSIX bodies live in
# the backends tree (backends/plexus-native carries the compiled shm mechanism,
# the gated asio reactor root under shm-include/.../linux carries the bridge), so
# after they moved out of lib/ no such primitive may appear under lib/ at all.
# The match runs against a comment-filtered view of each file (// line comments
# stripped, /* */ block-comment token mentions surfaced as a fatal error) because
# the generic ring headers describe the futex protocol in prose; the gate keys on
# actual #include / code tokens, never on comment text. This file is
# self-dispatching: included from the build it registers an ALL custom target
# that re-invokes it via -P (cross-platform, no bash dependency); run with
# -DLEAK_GATE_SCAN it performs the scan.

# The Linux shm primitives that may appear ONLY in the allowlisted backend dirs.
set(LEAK_TOKENS
    "<linux/futex.h>"
    "<sys/mman.h>"
    "<sys/eventfd.h>"
    "<sys/syscall.h>"
    "<liburing.h>"
    "shm_open"
    "io_uring"
    "SYS_futex")

if(NOT LEAK_GATE_SCAN)
    add_custom_target(shm_leak_gate ALL
        COMMAND ${CMAKE_COMMAND}
                -DLEAK_GATE_SCAN=ON
                -DLEAK_GATE_LIB_DIR=${PROJECT_SOURCE_DIR}/lib
                -DLEAK_GATE_BACKENDS_DIR=${PROJECT_SOURCE_DIR}/backends
                -P ${CMAKE_CURRENT_LIST_FILE}
        COMMENT "Asserting the generic library tree carries no platform shm primitives"
        VERBATIM)
    return()
endif()

# The dirs where the primitives legitimately live, excluded by path prefix. They
# sit under backends/, outside the lib/ scan below, so these excludes are a
# defensive record of the legitimate homes rather than load-bearing filters.
set(LEAK_ALLOWLIST
    "${LEAK_GATE_BACKENDS_DIR}/plexus-native/"
    "${LEAK_GATE_BACKENDS_DIR}/plexus-asio/shm-include/plexus/asio/shm/linux/")

file(GLOB_RECURSE _scan_files
    "${LEAK_GATE_LIB_DIR}/*.h"
    "${LEAK_GATE_LIB_DIR}/*.hpp"
    "${LEAK_GATE_LIB_DIR}/*.cpp"
    "${LEAK_GATE_LIB_DIR}/*.cc")

foreach(_prefix IN LISTS LEAK_ALLOWLIST)
    string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" _prefix_re "${_prefix}")
    list(FILTER _scan_files EXCLUDE REGEX "^${_prefix_re}")
endforeach()

function(_leak_line_token line out_token)
    set(${out_token} "" PARENT_SCOPE)
    foreach(_tok IN LISTS LEAK_TOKENS)
        string(FIND "${line}" "${_tok}" _pos)
        if(NOT _pos EQUAL -1)
            set(${out_token} "${_tok}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
endfunction()

foreach(_file IN LISTS _scan_files)
    file(STRINGS "${_file}" _lines)
    set(_in_block FALSE)
    set(_lineno 0)
    foreach(_line IN LISTS _lines)
        math(EXPR _lineno "${_lineno} + 1")

        if(_in_block)
            _leak_line_token("${_line}" _bt)
            if(_bt)
                message(FATAL_ERROR
                    "shm leak gate: block comment mentions '${_bt}' at ${_file}:${_lineno} "
                    "(project convention is // line comments only; a /* */ token mention is disallowed)")
            endif()
            string(FIND "${_line}" "*/" _close)
            if(NOT _close EQUAL -1)
                set(_in_block FALSE)
            endif()
            continue()
        endif()

        string(STRIP "${_line}" _stripped)
        if(_stripped MATCHES "^//")
            continue()
        endif()

        string(FIND "${_line}" "/*" _open)
        if(NOT _open EQUAL -1)
            string(FIND "${_line}" "*/" _close)
            if(_close EQUAL -1)
                set(_in_block TRUE)
            endif()
            _leak_line_token("${_line}" _bt)
            if(_bt)
                message(FATAL_ERROR
                    "shm leak gate: block comment mentions '${_bt}' at ${_file}:${_lineno} "
                    "(project convention is // line comments only; a /* */ token mention is disallowed)")
            endif()
            continue()
        endif()

        _leak_line_token("${_line}" _ct)
        if(_ct)
            message(FATAL_ERROR
                "shm leak gate: Linux shm primitive '${_ct}' in the generic tree at ${_file}:${_lineno} "
                "(it may appear only under backends/plexus-native or the gated asio shm-include/.../linux dir)")
        endif()
    endforeach()
endforeach()

list(LENGTH _scan_files _count)
message(STATUS "shm leak gate: clean (${_count} generic files scanned)")
