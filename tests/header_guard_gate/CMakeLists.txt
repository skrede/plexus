# The header-guard gate's own cases: each rejection's tree holds exactly one violation, of that kind, and the conforming
# tree, one header under each root, passes clean. The six path cases share one kind and each pins the guard its
# derivation branch gives; each of their trees also holds a conforming header under another root, so the gate reads it
# by its roots, not whole. The gate lists tracked files only, so a fixture counts once it is staged.
set(_kinds path define missing pragma endif collision)

function(_guard_case name kind pass_re)
    set(_others ${_kinds})
    list(REMOVE_ITEM _others ${kind})
    list(JOIN _others "|" _others)
    add_test(NAME header_guard_gate.${name}_rejected
        COMMAND ${CMAKE_COMMAND} -DHEADER_GATE_SCAN=ON -DHEADER_GATE_FATAL=ON
                -DHEADER_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/${name}
                -P ${PROJECT_SOURCE_DIR}/cmake/header_guard_gate.cmake)
    set_tests_properties(header_guard_gate.${name}_rejected PROPERTIES
        PASS_REGULAR_EXPRESSION "${pass_re}"
        FAIL_REGULAR_EXPRESSION ": (${_others}): ;gate: ([02-9]|[1-9][0-9]+) violation")
endfunction()

set(_derived ": path: HPP_GUARD_[A-Z0-9_]+, derived HPP_GUARD_")
_guard_case(form path "tests/form_fixture\\.h${_derived}TESTS_FORM_FIXTURE_H")
_guard_case(library path "lib/plexus-demo/include/plexus/demo/library_fixture\\.h${_derived}PLEXUS_DEMO_LIBRARY_FIXTURE_H")
_guard_case(example path "examples/mcu/esp-idf-demo/main/example_fixture\\.h${_derived}EXAMPLES_MCU_ESP_IDF_DEMO_MAIN_EXAMPLE_FIXTURE_H")
_guard_case(example_include path
    "examples/demo/include/plexus/demo/example_include_fixture\\.h${_derived}PLEXUS_DEMO_EXAMPLE_INCLUDE_FIXTURE_H")
_guard_case(template path
    "examples/mcu/esp-idf-demo/main/template_fixture\\.h\\.example${_derived}EXAMPLES_MCU_ESP_IDF_DEMO_MAIN_TEMPLATE_FIXTURE_H")
_guard_case(benchmark path "benchmarks/relay/benchmark_fixture\\.h${_derived}BENCHMARKS_RELAY_BENCHMARK_FIXTURE_H")
_guard_case(define define "tests/define_fixture\\.h: define: ")
_guard_case(missing missing "tests/missing_fixture\\.h: missing: ")
_guard_case(pragma pragma "tests/pragma_fixture\\.h: pragma: ")
_guard_case(endif endif "tests/endif_fixture\\.h: endif: ")
set(_shared "include/plexus/demo/collision_fixture\\.h")
_guard_case(collision collision
    "lib/plexus-demo/${_shared}: collision: HPP_GUARD_PLEXUS_DEMO_COLLISION_FIXTURE_H, also derived by examples/demo/${_shared}")

add_test(NAME header_guard_gate.conforming_accepted
    COMMAND ${CMAKE_COMMAND} -DHEADER_GATE_SCAN=ON -DHEADER_GATE_FATAL=ON
            -DHEADER_GATE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/conforming
            -P ${PROJECT_SOURCE_DIR}/cmake/header_guard_gate.cmake)
set_tests_properties(header_guard_gate.conforming_accepted PROPERTIES
    PASS_REGULAR_EXPRESSION "header guard gate: clean \\(8 headers checked\\)")
