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

add_subdirectory(function)
