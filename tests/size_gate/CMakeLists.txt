# The size gate's own rejections: an unregistered overage of a C++, CMakeLists.txt, CMake, CMake template, shell,
# Python or Lua file, a registered C++ or shell file grown past its ceiling, a second row for one path, and a
# path-shaped row under a non-file kind.
add_test(NAME size_gate.unregistered_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
            -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
set_tests_properties(size_gate.unregistered_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "over_limit_fixture\\.h: [0-9]+ lines over the 200 ceiling with no EXCEPTIONS\\.md row")

add_test(NAME size_gate.ceiling_growth_rejected
    COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
            -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/ceiling
            -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
set_tests_properties(size_gate.ceiling_growth_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "grown_fixture\\.h: 221 lines over its registered ceiling 220"
    FAIL_REGULAR_EXPRESSION "held_fixture\\.h")

add_test(NAME size_gate.duplicate_row_rejected
    COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
            -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/duplicate
            -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
set_tests_properties(size_gate.duplicate_row_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "duplicate_fixture\\.h: duplicate EXCEPTIONS\\.md row")

add_test(NAME size_gate.non_file_kind_rejected
    COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
            -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/kind
            -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
set_tests_properties(size_gate.non_file_kind_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "kind_fixture\\.h: EXCEPTIONS\\.md row kind 'function' is not file")

set(_script_kinds cmakelists cmake cmake_in shell python lua)
set(_script_fixtures CMakeLists\\.txt over_limit_fixture\\.cmake over_limit_fixture\\.cmake\\.in
    over_limit_fixture\\.sh over_limit_fixture\\.py over_limit_fixture\\.lua)
foreach(_kind _fixture IN ZIP_LISTS _script_kinds _script_fixtures)
    add_test(NAME size_gate.${_kind}_overage_rejected
        COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
                -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/scripts
                -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
    set_tests_properties(size_gate.${_kind}_overage_rejected PROPERTIES
        PASS_REGULAR_EXPRESSION "[ /]${_fixture}: [0-9]+ lines over the 200 ceiling with no EXCEPTIONS\\.md row")
endforeach()

add_test(NAME size_gate.script_ceiling_growth_rejected
    COMMAND ${CMAKE_COMMAND} -DSIZE_GATE_SCAN=ON -DSIZE_GATE_FATAL=ON
            -DSIZE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/script_ceiling
            -P ${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
set_tests_properties(size_gate.script_ceiling_growth_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "grown_fixture\\.sh: 221 lines over its registered ceiling 220"
    FAIL_REGULAR_EXPRESSION "held_fixture\\.cmake|CMakeLists\\.txt")

# The function gate's parsing of a recorded clang-tidy log: an unbalanced bracket in a quoted source line must
# neither fold test-only warnings into a library violation nor hide a real library overage.
add_test(NAME function_size_gate.bracket_noise_ignored
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/function/bracket_noise.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.bracket_noise_ignored PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: clean")

add_test(NAME function_size_gate.library_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/function/library_overage.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.library_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- /src/backends/plexus-asio/include/plexus/asio/long_body\\.h:20:5: warning: function 'long_body' exceeds"
    FAIL_REGULAR_EXPRESSION "dummyFunction1")

# The function gate's compile-database coverage: a library source the database lacks is named, and a database
# holding every one passes.
add_test(NAME function_size_gate.coverage_missing_tu_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/function/bracket_noise.txt
            -DFUNC_GATE_BUILD_DIR=${CMAKE_CURRENT_SOURCE_DIR}/function/database
            -DFUNC_GATE_SOURCES_FILE=${CMAKE_CURRENT_SOURCE_DIR}/function/database/sources_missing.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.coverage_missing_tu_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: /src/backends/plexus-demo/src/c\\.cpp is compiled by a library target but missing from the compile database"
    FAIL_REGULAR_EXPRESSION "src/(a|b)\\.cpp is compiled")

add_test(NAME function_size_gate.coverage_complete_accepted
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_SOURCE_DIR}/function/bracket_noise.txt
            -DFUNC_GATE_BUILD_DIR=${CMAKE_CURRENT_SOURCE_DIR}/function/database
            -DFUNC_GATE_SOURCES_FILE=${CMAKE_CURRENT_SOURCE_DIR}/function/database/sources_complete.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.coverage_complete_accepted PROPERTIES
    PASS_REGULAR_EXPRESSION "function size gate: clean")

# The function gate counts a function as it sits on screen: a template header puts a body that clang-tidy measures
# within the ceiling over it, and a function that fits is not named.
configure_file(function/screen/screen_tidy.txt.in ${CMAKE_CURRENT_BINARY_DIR}/function/screen_tidy.txt @ONLY)
add_test(NAME function_size_gate.screen_count_overage_rejected
    COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON
            -DFUNC_GATE_TIDY_LOG=${CMAKE_CURRENT_BINARY_DIR}/function/screen_tidy.txt
            -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
set_tests_properties(function_size_gate.screen_count_overage_rejected PROPERTIES
    PASS_REGULAR_EXPRESSION "1 unsuppressed library function overage\\(s\\):[^/]*- [^ ]*/screen_fixture\\.h:9:3: warning: function 'over_on_screen' exceeds [^\n]* -- 26 lines on screen"
    FAIL_REGULAR_EXPRESSION "within_on_screen")

# The function gate fails closed on a clang-tidy run that measured nothing: run-clang-tidy without a database, a
# database without a source, and a library source clang-tidy cannot open. Each runs the real tool, so each needs it.
find_program(RUN_CLANG_TIDY NAMES run-clang-tidy run-clang-tidy.py)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND RUN_CLANG_TIDY)
    set(_run_dir ${CMAKE_CURRENT_BINARY_DIR}/function/run)
    file(MAKE_DIRECTORY ${_run_dir}/no_database)
    file(WRITE ${_run_dir}/empty_database/compile_commands.json "[]\n")
    set(_missing ${_run_dir}/missing_source/backends/plexus-demo/src/missing.cpp)
    file(WRITE ${_run_dir}/missing_source/compile_commands.json
        "[{\"directory\": \"${_run_dir}/missing_source\", \"command\": \"c++ -std=c++20 -c ${_missing}\", \"file\": \"${_missing}\"}]\n")
    set(_run_cases no_database empty_database missing_source)
    set(_run_verdicts "run-clang-tidy failed \\(exit [0-9]+\\)" "clang-tidy measured no function"
        "clang-tidy could not check a library source:[^/]*Error while processing [^ ]*/missing_source/backends/plexus-demo/src/missing\\.cpp")
    foreach(_case _verdict IN ZIP_LISTS _run_cases _run_verdicts)
        add_test(NAME function_size_gate.${_case}_run_rejected
            COMMAND ${CMAKE_COMMAND} -DFUNC_GATE_SCAN=ON -DFUNC_GATE_FATAL=ON -DFUNC_GATE_BUILD_DIR=${_run_dir}/${_case}
                    -P ${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)
        set_tests_properties(function_size_gate.${_case}_run_rejected PROPERTIES PASS_REGULAR_EXPRESSION "function size gate: ${_verdict}")
    endforeach()
endif()
