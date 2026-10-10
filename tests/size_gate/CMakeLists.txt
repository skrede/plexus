# The size gate's own rejections: an unregistered overage and a registered file grown past its ceiling.
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
