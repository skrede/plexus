# Driven as a ctest entry (cmake -P). Installs the already-configured plexus build
# tree to a throwaway prefix, then configures + builds + RUNS the out-of-tree consumer
# project against that prefix via find_package(plexus COMPONENTS ...). Any nonzero step is
# fatal, so a broken export set, a missing header, an unresolved transitive link, or a
# crypto symbol that links but fails to run fails the test. Required: -DPLEXUS_BUILD_DIR
# (the configured build tree) and -DPLEXUS_CONSUME_DIR (the consumer project source).
# Optional: -DPLEXUS_CONSUME_COMPONENTS (the build's enabled backends, semicolon list).

if(NOT DEFINED PLEXUS_BUILD_DIR OR NOT DEFINED PLEXUS_CONSUME_DIR)
    message(FATAL_ERROR "install_consume.cmake requires -DPLEXUS_BUILD_DIR and -DPLEXUS_CONSUME_DIR")
endif()

set(_prefix "${PLEXUS_BUILD_DIR}/node-install-consume/prefix")
set(_consume_build "${PLEXUS_BUILD_DIR}/node-install-consume/build")
file(REMOVE_RECURSE "${_prefix}" "${_consume_build}")

execute_process(
    COMMAND ${CMAKE_COMMAND} --install "${PLEXUS_BUILD_DIR}" --prefix "${_prefix}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "install failed (${_rc}):\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${PLEXUS_CONSUME_DIR}" -B "${_consume_build}"
            "-DCMAKE_PREFIX_PATH=${_prefix}"
            "-DPLEXUS_CONSUME_COMPONENTS=${PLEXUS_CONSUME_COMPONENTS}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer configure failed (${_rc}):\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_consume_build}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer build failed (${_rc}):\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_CTEST_COMMAND} --test-dir "${_consume_build}" --output-on-failure
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer run failed (${_rc}):\n${_out}")
endif()

message(STATUS "install+consume OK (components: ${PLEXUS_CONSUME_COMPONENTS})")
