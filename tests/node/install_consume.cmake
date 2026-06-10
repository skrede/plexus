# Driven as a ctest entry (cmake -P). Installs the already-configured plexus build
# tree to a throwaway prefix, then configures + builds the out-of-tree consumer
# project against that prefix via find_package(plexus). Any nonzero step is fatal,
# so a broken export set, a missing header, or an unresolved transitive link fails
# the test. Required: -DPLEXUS_BUILD_DIR (the configured build tree) and
# -DPLEXUS_CONSUME_DIR (the consumer project source).

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

message(STATUS "install+consume OK")
