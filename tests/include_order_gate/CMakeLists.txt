# The include-order gate's own rejections: each kind's fixture directory holds exactly one violation, of that kind, and
# the conforming fixture, using every section, passes clean.
set(_kinds section folder split own length alpha blank)
foreach(_kind IN LISTS _kinds)
    set(_others ${_kinds})
    list(REMOVE_ITEM _others ${_kind})
    list(JOIN _others "|" _others)
    add_test(NAME include_order_gate.${_kind}_rejected
        COMMAND ${CMAKE_COMMAND} -DINCLUDE_GATE_SCAN=ON -DINCLUDE_GATE_FATAL=ON
                -DINCLUDE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/${_kind}
                -P ${PROJECT_SOURCE_DIR}/cmake/include_order_gate.cmake)
    set_tests_properties(include_order_gate.${_kind}_rejected PROPERTIES
        PASS_REGULAR_EXPRESSION "${_kind}_fixture\\.h:[0-9]+: ${_kind}:"
        FAIL_REGULAR_EXPRESSION ":[0-9]+: (${_others}): ;gate: ([02-9]|[1-9][0-9]+) violation")
endforeach()

add_test(NAME include_order_gate.conforming_accepted
    COMMAND ${CMAKE_COMMAND} -DINCLUDE_GATE_SCAN=ON -DINCLUDE_GATE_FATAL=ON
            -DINCLUDE_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/conforming
            -P ${PROJECT_SOURCE_DIR}/cmake/include_order_gate.cmake)
set_tests_properties(include_order_gate.conforming_accepted PROPERTIES PASS_REGULAR_EXPRESSION "include order gate: clean")
