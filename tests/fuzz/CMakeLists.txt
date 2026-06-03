# Wire attack-surface libFuzzer harnesses. Each harness drives a header-only
# decoder over fuzzer-controlled spans; the blast radius is just plexus::wire.
# libFuzzer requires a Clang toolchain — skip with a warning otherwise.
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING
        "plexus fuzz: PLEXUS_BUILD_FUZZ_TESTS is ON but the compiler is "
        "${CMAKE_CXX_COMPILER_ID}; libFuzzer needs Clang. Skipping harnesses.")
    return()
endif()

function(plexus_add_fuzz_target name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE plexus::wire)
    target_compile_options(${name} PRIVATE
        -fsanitize=fuzzer,address
        -fno-sanitize-recover=all
        -fno-omit-frame-pointer
        -g
        -O1)
    target_link_options(${name} PRIVATE -fsanitize=fuzzer,address)
endfunction()

plexus_add_fuzz_target(fuzz_frame_reassembler)
plexus_add_fuzz_target(fuzz_data_frame)
