# Linking only core and inproc keeps the include path to those two trees, so a core header reaching for a facade or
# backend header fails to compile. wire comes through core: linking it here would mask a broken core-to-wire link.
add_executable(core_standalone core_standalone.cpp)
target_link_libraries(core_standalone PRIVATE plexus::core plexus::inproc)
add_test(NAME node.core_standalone COMMAND core_standalone)

# With no shm library linked the shm include tree is off the path, so a facade header reaching for an shm header, or
# naming an shm:: symbol on this plain pack, fails to compile. The proof is the build; it runs only to be tracked.
add_executable(core_only_node_compile test_core_only_node_compile.cpp)
target_link_libraries(core_only_node_compile PRIVATE plexus::plexus plexus::core plexus::discovery)
add_test(NAME node.core_only_node_compile COMMAND core_only_node_compile)

# A real install and an out-of-tree find_package consumer; the components mirror this build's enabled backends so the
# check proves exactly the export sets that shipped.
set(_consume_components "")
if(PLEXUS_ENABLE_ASIO_BACKEND)
    list(APPEND _consume_components asio)
endif()
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    list(APPEND _consume_components crypto)
endif()
if(PLEXUS_ENABLE_SHM_BACKEND)
    list(APPEND _consume_components native)
endif()
# The parent's toolchain is forwarded because cmake's platform default differs: on Windows it is a multi-config Visual
# Studio generator, which does not match this Ninja + cl build and would not inherit the vcvars environment.
add_test(NAME node.install_consume
    COMMAND ${CMAKE_COMMAND}
        -DPLEXUS_BUILD_DIR=${CMAKE_BINARY_DIR}
        -DPLEXUS_CONSUME_DIR=${CMAKE_CURRENT_SOURCE_DIR}/install_consume
        "-DPLEXUS_CONSUME_COMPONENTS=${_consume_components}"
        "-DPLEXUS_CONSUME_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
        "-DPLEXUS_CONSUME_GENERATOR=${CMAKE_GENERATOR}"
        "-DPLEXUS_CONSUME_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
        "-DPLEXUS_CONSUME_C_COMPILER=${CMAKE_C_COMPILER}"
        "-DPLEXUS_CONSUME_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
        -P ${CMAKE_CURRENT_SOURCE_DIR}/install_consume.cmake)
