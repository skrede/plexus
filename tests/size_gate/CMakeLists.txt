# The size gate's own rejections: an unregistered overage, a registered file grown past its ceiling,
# a second row for one path, and a path-shaped row under a non-file kind.
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
