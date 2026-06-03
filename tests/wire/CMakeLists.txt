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
    # Prefix the ctest registration so `ctest -R wire` selects the whole wire
    # suite by name (the [wire] Catch2 tag is not a ctest-visible selector).
    catch_discover_tests(${test} TEST_PREFIX "wire.")
endforeach()
