# The function gate's parsing of a recorded clang-tidy log: an unbalanced bracket in a quoted source line must
# neither fold test-only warnings into a library violation nor hide a real library overage.
add_test(NAME function_size_gate.bracket_noise_ignored
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/bracket_noise.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.bracket_noise_ignored PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: clean")

add_test(NAME function_size_gate.library_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/library_overage.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.library_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- /src/backends/plexus-asio/include/plexus/asio/long_body\\.h:20:5: warning: function 'long_body' exceeds"
    FAIL_REGULAR_EXPRESSION "dummyFunction1")

# The function gate's compile-database coverage: a library source the database lacks is named, and a database
# holding every one passes.
add_test(NAME function_size_gate.coverage_missing_tu_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/bracket_noise.txt
            -DFUNC_GATE_BUILD_DIR=${CMAKE_CURRENT_SOURCE_DIR}/database
            -DFUNC_GATE_SOURCES_FILE=${CMAKE_CURRENT_SOURCE_DIR}/database/sources_missing.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.coverage_missing_tu_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: /src/backends/plexus-demo/src/c\\.cpp is compiled by a library target but missing from the compile database"
    FAIL_REGULAR_EXPRESSION "src/(a|b)\\.cpp is compiled")

add_test(NAME function_size_gate.coverage_complete_accepted
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/bracket_noise.txt
            -DFUNC_GATE_BUILD_DIR=${CMAKE_CURRENT_SOURCE_DIR}/database
            -DFUNC_GATE_SOURCES_FILE=${CMAKE_CURRENT_SOURCE_DIR}/database/sources_complete.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.coverage_complete_accepted PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: clean")

# The function gate counts a function as it sits on screen: a template header puts a body that clang-tidy measures
# within the ceiling over it, and a function that fits is not named.
configure_file(screen/screen_tidy.txt.in ${CMAKE_CURRENT_BINARY_DIR}/screen_tidy.txt @ONLY)
add_test(NAME function_size_gate.screen_count_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_BINARY_DIR}/screen_tidy.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.screen_count_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- [^ ]*/screen_fixture\\.h:9:3: warning: function 'over_on_screen' exceeds [^\n]* -- 26 lines on screen"
    FAIL_REGULAR_EXPRESSION "within_on_screen")

# A constructor's member init list counts on screen though clang-tidy measures only its body; the second constructor
# fits at 25. A function laid out off the clang-format shape cannot be measured, and that fails the gate.
configure_file(screen/init_list_tidy.txt.in ${CMAKE_CURRENT_BINARY_DIR}/init_list_tidy.txt @ONLY)
add_test(NAME function_size_gate.init_list_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_BINARY_DIR}/init_list_tidy.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.init_list_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- [^ ]*/init_list_fixture\\.h:11:14: warning: function 'init_list_fixture' exceeds [^\n]* -- 26 lines on screen"
    FAIL_REGULAR_EXPRESSION "init_list_fixture\\.h:38:")

configure_file(screen/unmeasurable_tidy.txt.in ${CMAKE_CURRENT_BINARY_DIR}/unmeasurable_tidy.txt @ONLY)
add_test(NAME function_size_gate.unmeasurable_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_BINARY_DIR}/unmeasurable_tidy.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.unmeasurable_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- [^ ]*/unmeasurable_fixture\\.h:9:22: warning: function 'brace_on_signature_line' exceeds [^\n]* -- not measurable from the source")

# The run check over a recorded run that exits nonzero with clang-tidy's per-TU lines in its error output, where an
# older run-clang-tidy writes them: those lines pass and are never echoed, while a library source left unprocessed,
# a clang-tidy that died, or any other error output fails the gate.
configure_file(stderr/measured_tidy.txt.in ${CMAKE_CURRENT_BINARY_DIR}/measured_tidy.txt @ONLY)
set(_stderr_cases noise_accepted library_error_rejected signal_rejected residue_rejected)
set(_stderr_fixtures noise library_error signal residue)
set(_stderr_verdicts "clean"
    "clang-tidy did not finish a source:[^/]*Error while processing /src/backends/plexus-demo/src/broken\\.cpp"
    "clang-tidy did not finish a source:[^/]*/src/tests/node/crash\\.cpp: terminated by signal 11"
    "run-clang-tidy failed \\(exit 1\\):[^/]*Unable to run clang-tidy\\.")
foreach(_case _fixture _verdict IN ZIP_LISTS _stderr_cases _stderr_fixtures _stderr_verdicts)
    add_test(NAME function_size_gate.stderr_${_case}
        COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
                -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_BINARY_DIR}/measured_tidy.txt -DFUNC_GATE_TIDY_RC=1
                -DFUNC_GATE_TIDY_ERR=${CMAKE_CURRENT_SOURCE_DIR}/stderr/${_fixture}.txt
                -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
    set_tests_properties(function_size_gate.stderr_${_case} PROPERTIES
        PASS_REGULAR_EXPRESSION "function size gate: ${_verdict}" FAIL_REGULAR_EXPRESSION "warnings generated")
endforeach()

# The function gate fails closed on a clang-tidy run that measured nothing: run-clang-tidy without a database, a
# database without a source, and a library source clang-tidy cannot open. Each runs the real tool, so each needs it.
find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND RUN_CLANG_TIDY)
    set(_run_dir ${CMAKE_CURRENT_BINARY_DIR}/run)
    file(MAKE_DIRECTORY ${_run_dir}/no_database)
    file(WRITE ${_run_dir}/empty_database/compile_commands.json "[]\n")
    set(_missing ${_run_dir}/missing_source/backends/plexus-demo/src/missing.cpp)
    file(WRITE ${_run_dir}/missing_source/compile_commands.json
        "[{\"directory\": \"${_run_dir}/missing_source\", \"command\": \"c++ -std=c++20 -c ${_missing}\", \"file\": \"${_missing}\"}]\n")
    set(_run_cases no_database empty_database missing_source)
    set(_run_verdicts "run-clang-tidy failed \\(exit [0-9]+\\)" "clang-tidy measured no function"
        "clang-tidy did not finish a source:[^/]*Error while processing [^ ]*/missing_source/backends/plexus-demo/src/missing\\.cpp")
    foreach(_case _verdict IN ZIP_LISTS _run_cases _run_verdicts)
        add_test(NAME function_size_gate.${_case}_run_rejected
            COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON -DFUNC_GATE_BUILD_DIR=${_run_dir}/${_case}
                    -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
        set_tests_properties(function_size_gate.${_case}_run_rejected PROPERTIES PASS_REGULAR_EXPRESSION "function size gate: ${_verdict}")
    endforeach()
endif()
