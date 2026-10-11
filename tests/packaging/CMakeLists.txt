# Packaging legs: each configures the real tree in a scratch work dir, the way a downstream
# project consumes it, and builds a compile-only consumer against it.
if(NOT LINUX OR NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

include(FetchContent)
FetchContent_GetProperties(asio)
set(_packaging_asio "")
if(asio_POPULATED)
    set(_packaging_asio -DASIO_SOURCE_DIR=${asio_SOURCE_DIR})
endif()

set(_packaging_inputs
    -DPLEXUS_SOURCE_DIR=${PROJECT_SOURCE_DIR}
    "-DPROBE_GENERATOR=${CMAKE_GENERATOR}"
    -DPROBE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    "-DPROBE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}"
    ${_packaging_asio})

add_test(NAME build.shm_off
    COMMAND ${CMAKE_COMMAND}
        ${_packaging_inputs}
        -DPROBE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/shm_off
        -DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/shm_off
        -P ${CMAKE_CURRENT_SOURCE_DIR}/run_shm_off.cmake)
set_tests_properties(build.shm_off PROPERTIES TIMEOUT 900 LABELS packaging)

add_test(NAME build.install_consume
    COMMAND ${CMAKE_COMMAND}
        ${_packaging_inputs}
        -DPROBE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/install_consumer
        -DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/install_consume
        -P ${CMAKE_CURRENT_SOURCE_DIR}/run_install_consume.cmake)
set_tests_properties(build.install_consume PROPERTIES TIMEOUT 1200 LABELS packaging SKIP_REGULAR_EXPRESSION "PLEXUS_SKIP")
