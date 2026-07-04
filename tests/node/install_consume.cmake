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

# Forward the parent's exact toolchain so the nested configure matches this build rather
# than cmake's platform default (a multi-config Visual Studio generator on Windows, which
# neither matches this single-config Ninja + cl build nor inherits the vcvars environment).
set(_toolchain_args "")
if(PLEXUS_CONSUME_GENERATOR)
    list(APPEND _toolchain_args -G "${PLEXUS_CONSUME_GENERATOR}")
endif()
if(PLEXUS_CONSUME_MAKE_PROGRAM)
    list(APPEND _toolchain_args "-DCMAKE_MAKE_PROGRAM=${PLEXUS_CONSUME_MAKE_PROGRAM}")
endif()
if(PLEXUS_CONSUME_C_COMPILER)
    list(APPEND _toolchain_args "-DCMAKE_C_COMPILER=${PLEXUS_CONSUME_C_COMPILER}")
endif()
if(PLEXUS_CONSUME_CXX_COMPILER)
    list(APPEND _toolchain_args "-DCMAKE_CXX_COMPILER=${PLEXUS_CONSUME_CXX_COMPILER}")
endif()

# Forward the build's CXX flags to the consumer. When plexus is built with sanitizers,
# the installed crypto OBJECT files carry __asan_/__ubsan_ references that only resolve
# when the consumer link pulls in the matching sanitizer runtime — so the out-of-tree
# consumer must compile with the same instrumentation, not a bare default.
execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${PLEXUS_CONSUME_DIR}" -B "${_consume_build}"
            ${_toolchain_args}
            "-DCMAKE_PREFIX_PATH=${_prefix}"
            "-DPLEXUS_CONSUME_COMPONENTS=${PLEXUS_CONSUME_COMPONENTS}"
            "-DCMAKE_CXX_FLAGS=${PLEXUS_CONSUME_CXX_FLAGS}"
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
