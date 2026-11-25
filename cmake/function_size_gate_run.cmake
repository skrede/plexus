# clang-tidy exits nonzero on a TU that fails to compile, as the compile-fail tests' TUs do by design, and
# run-clang-tidy before LLVM 19 writes clang-tidy's per-TU summary lines to its own error output. A run is broken when
# a clang-tidy dies or a library TU cannot be processed, as either output says, when it exits nonzero with error output
# beyond those summary lines, or when it measured no function.
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
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C grep -a -E "${_diagnostic_regex}|terminated by signal|^Error while processing "
        COMMAND ${CMAKE_COMMAND} -E env LC_ALL=C awk "!seen[$0]++"
        OUTPUT_FILE ${FUNC_GATE_BUILD_DIR}/${name}.log
        ERROR_FILE ${FUNC_GATE_BUILD_DIR}/${name}.err
        RESULTS_VARIABLE _results)
    list(GET _results 0 _rc)
    set(${rc_out} ${_rc} PARENT_SCOPE)
endfunction()
