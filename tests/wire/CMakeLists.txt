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

# The handshake encode-into NOALLOC gate is isolated in its own executable:
# support/alloc_counter.h overrides global operator new/delete once per program,
# so it must not share a binary with the rest of the wire oracle. tests/ is on the
# include path so it can pull in support/alloc_counter.h.
add_executable(test_handshake_codec_noalloc test_handshake_codec_noalloc.cpp)
target_link_libraries(test_handshake_codec_noalloc PRIVATE plexus::wire Catch2::Catch2WithMain)
target_include_directories(test_handshake_codec_noalloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_handshake_codec_noalloc TEST_PREFIX "wire.")
