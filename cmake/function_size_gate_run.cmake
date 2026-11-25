# clang-tidy exits nonzero on a TU that fails to compile, as the compile-fail tests' TUs do by design, and
# run-clang-tidy before LLVM 19 writes clang-tidy's per-TU summary lines to its own error output. A run is broken when
# a clang-tidy dies on a source and again on its one rerun, or a library TU cannot be processed, as either output says,
# when it exits nonzero with error output beyond those summary lines, or when it measured no function.
function(function_size_gate_check_run log rc err)
    set(_failure_regex "terminated by signal|^Error while processing .*${FUNC_GATE_SCOPE}")
    string(CONCAT _summary_regex "^([0-9]+ (warnings?|errors?|warnings? and [0-9]+ errors?) generated\\.|"
        "Error while processing .*|Suppressed [0-9]+ warnings? .*|Use -header-filter=.*|"
        "Found compiler error\\(s\\)\\.|[ \t]*)$")
    file(STRINGS "${log}" _failures REGEX "${_failure_regex}")
    file(STRINGS "${err}" _err_failures REGEX "${_failure_regex}")
    list(APPEND _failures ${_err_failures})
    file(STRINGS "${err}" _residue)
    list(FILTER _residue EXCLUDE REGEX "${_summary_regex}")
    file(STRINGS "${log}" _measured REGEX ": note: [0-9]+ lines including" LIMIT_COUNT 1)
    if(_failures)
        list(JOIN _failures "\n  " _failures)
        message(FATAL_ERROR "function size gate: clang-tidy did not finish a source:\n  ${_failures}")
    elseif(NOT rc EQUAL 0 AND NOT _residue STREQUAL "")
        list(JOIN _residue "\n  " _residue)
        message(FATAL_ERROR "function size gate: run-clang-tidy failed (exit ${rc}):\n  ${_residue}")
    elseif(NOT _measured)
        message(FATAL_ERROR "function size gate: clang-tidy measured no function")
    endif()
endfunction()

# -checks keeps an unrelated diagnostic out of the result. clang-tidy counts a body from brace to brace, so a zero
# line threshold reports every body's length for the screen count; -config replaces .clang-tidy, hence the
# restated limits. The log keeps only the lines the gate reads, each once: a header's diagnostics repeat in every TU
# that includes it, which made the raw output hundreds of megabytes. The filters run in the C locale so a source
# line's non-UTF-8 byte cannot make grep treat the stream as binary. FUNC_GATE_TIDY_BINARY replaces clang-tidy, so a
# test can make it crash.
function(function_size_gate_tidy name rc_out)
    set(_binary "")
    if(FUNC_GATE_TIDY_BINARY)
        set(_binary -clang-tidy-binary "${FUNC_GATE_TIDY_BINARY}")
    endif()
    string(CONCAT _tidy_config "{CheckOptions: {"
        "readability-function-size.LineThreshold: 0, readability-function-size.StatementThreshold: -1, "
        "readability-function-cognitive-complexity.Threshold: 25, readability-function-cognitive-complexity.IgnoreMacros: true}}")
    execute_process(
        COMMAND ${RUN_CLANG_TIDY} ${_binary}
                -p ${FUNC_GATE_BUILD_DIR}
                -quiet
                -checks=-*,readability-function-size,readability-function-cognitive-complexity
                "-config=${_tidy_config}"
                "-header-filter=.*/(lib|backends)/plexus.*"
                ${ARGN}
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C grep -a -E "${_diagnostic_regex}|terminated by signal|^Error while processing "
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C awk "!seen[$0]++"
        OUTPUT_FILE ${FUNC_GATE_BUILD_DIR}/${name}.log
        ERROR_FILE ${FUNC_GATE_BUILD_DIR}/${name}.err
        RESULTS_VARIABLE _results)
    list(GET _results 0 _rc)
    set(${rc_out} ${_rc} PARENT_SCOPE)
endfunction()

# A line run together with another process's output names no source that exists, so it is never taken for one to rerun.
function(function_size_gate_crashes name out)
    file(STRINGS "${FUNC_GATE_BUILD_DIR}/${name}.log" _lines REGEX ": terminated by signal [0-9]+$")
    file(STRINGS "${FUNC_GATE_BUILD_DIR}/${name}.err" _err_lines REGEX ": terminated by signal [0-9]+$")
    set(_crashes "")
    foreach(_line IN LISTS _lines _err_lines)
        string(REGEX REPLACE ": terminated by signal [0-9]+$" "" _source "${_line}")
        if(EXISTS "${_source}")
            list(APPEND _crashes "${_line}")
        endif()
    endforeach()
    set(${out} "${_crashes}" PARENT_SCOPE)
endfunction()

# The first run without the signal lines of the sources it reran and without clang's crash reports, then the rerun in
# full. A crash report goes whether or not its source died again, since a second death fails on its own signal line.
function(function_size_gate_judged crashes)
    set(_skip "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy_skip.txt")
    list(JOIN crashes "\n" _skipped)
    file(WRITE "${_skip}" "${_skipped}\n")
    string(CONCAT _report "^(PLEASE submit a bug report .*|Stack dump.*:|[0-9]+\\.\t.*|"
        " *#[0-9]+ 0x[0-9a-f]+ .*|[0-9]+ +[^ ]+ +0x[0-9a-f]+ .*)$")
    foreach(_output log err)
        set(_judged "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy_judged.${_output}")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C grep -a -v -x -F -f "${_skip}" "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy.${_output}"
            COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C grep -a -v -E "${_report}"
            OUTPUT_FILE "${_judged}")
        file(READ "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy_rerun.${_output}" _rerun)
        file(APPEND "${_judged}" "${_rerun}")
    endforeach()
endfunction()

# Each source a clang-tidy died on is rerun once, after the full run, and the judged run replaces the first.
# run-clang-tidy searches its positional regular expressions in the database's file entries, so each is anchored.
function(function_size_gate_rerun_crashed rc_var log_var err_var)
    function_size_gate_crashes(function_size_gate_tidy _crashes)
    if(NOT _crashes)
        return()
    endif()
    list(TRANSFORM _crashes REPLACE ": terminated by signal [0-9]+$" "" OUTPUT_VARIABLE _sources)
    list(TRANSFORM _sources REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" OUTPUT_VARIABLE _patterns)
    list(TRANSFORM _patterns PREPEND "^")
    list(TRANSFORM _patterns APPEND "$")
    list(LENGTH _sources _count)
    list(JOIN _sources "\n  " _listed)
    message(STATUS "function size gate: reran ${_count} source(s) clang-tidy crashed on:\n  ${_listed}")
    function_size_gate_tidy(function_size_gate_tidy_rerun _rerun_rc ${_patterns})
    function_size_gate_judged("${_crashes}")
    if(NOT _rerun_rc EQUAL 0)
        set(${rc_var} ${_rerun_rc} PARENT_SCOPE)
    endif()
    set(${log_var} "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy_judged.log" PARENT_SCOPE)
    set(${err_var} "${FUNC_GATE_BUILD_DIR}/function_size_gate_tidy_judged.err" PARENT_SCOPE)
endfunction()
