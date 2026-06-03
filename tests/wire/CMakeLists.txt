# Wire-layer unit tests: the codec/reassembler/length-prefix parity oracle.
# Each links the header-only plexus::wire target plus Catch2's main; ctest -R
# wire selects them.
set(PLEXUS_WIRE_TESTS
    test_wire_codec
    test_frame_reassembler
    test_length_prefixed_helper)

foreach(test ${PLEXUS_WIRE_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::wire Catch2::Catch2WithMain)
    catch_discover_tests(${test})
endforeach()
