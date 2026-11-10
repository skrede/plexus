# Run through cmake -P: installs the configured build tree to a throwaway prefix, then configures, builds and runs
# the consumer project against it. -DPLEXUS_CONSUME_COMPONENTS optionally lists the build's enabled backends.

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

file(GLOB_RECURSE _targets_file "${_prefix}/plexusTargets.cmake")
if(NOT _targets_file)
    message(FATAL_ERROR "the install wrote no plexusTargets.cmake under ${_prefix}")
endif()
file(READ "${_targets_file}" _targets_text)
string(FIND "${_targets_text}" "$<CONFIG:Debug>" _per_config)
if(_per_config EQUAL -1)
    message(FATAL_ERROR "the exported plexus::core decides PLEXUS_AFFINITY_CHECKS at the producer's configure time "
                        "instead of in the consumer's configuration (${_targets_file})")
endif()

# The parent's exact toolchain, because cmake's default on Windows is a multi-config Visual Studio generator that
# neither matches this single-config Ninja + cl build nor inherits the vcvars environment.
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

# A sanitizer build leaves __asan_/__ubsan_ references in the installed crypto objects, which resolve only when the
# consumer compiles with the same instrumentation.
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
