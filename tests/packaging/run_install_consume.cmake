foreach(_input IN ITEMS PLEXUS_SOURCE_DIR PROBE_DIR WORK_DIR PROBE_GENERATOR PROBE_CXX_COMPILER)
    if(NOT DEFINED ${_input})
        message(FATAL_ERROR "run_install_consume.cmake needs -D${_input}")
    endif()
endforeach()

file(REMOVE_RECURSE "${WORK_DIR}")

set(_toolchain_args -G "${PROBE_GENERATOR}" "-DCMAKE_CXX_COMPILER=${PROBE_CXX_COMPILER}")
if(PROBE_CXX_COMPILER_LAUNCHER)
    list(APPEND _toolchain_args "-DCMAKE_CXX_COMPILER_LAUNCHER=${PROBE_CXX_COMPILER_LAUNCHER}")
endif()

set(_plexus_args
    -S "${PLEXUS_SOURCE_DIR}" -B "${WORK_DIR}/build" ${_toolchain_args}
    "-DCMAKE_INSTALL_PREFIX=${WORK_DIR}/prefix"
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON -DPLEXUS_ENABLE_CRYPTO_BACKEND=OFF
    -DPLEXUS_BUILD_TESTS=OFF -DPLEXUS_BUILD_EXAMPLES=OFF -DPLEXUS_BUILD_BENCHMARKS=OFF
    -DPLEXUS_BUILD_MCAP_TRANSCODE=OFF -DPLEXUS_BUILD_PCAP_TRANSCODE=OFF)
if(ASIO_SOURCE_DIR)
    list(APPEND _plexus_args "-DFETCHCONTENT_SOURCE_DIR_ASIO=${ASIO_SOURCE_DIR}")
endif()

function(run_step _label)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        string(LENGTH "${_out}" _len)
        if(_len GREATER 4000)
            math(EXPR _from "${_len} - 4000")
            string(SUBSTRING "${_out}" ${_from} -1 _out)
        endif()
        message(FATAL_ERROR "${_label} failed (${_rc}):\n${_out}")
    endif()
    set(_step_output "${_out}" PARENT_SCOPE)
endfunction()

run_step("plexus configure" ${CMAKE_COMMAND} ${_plexus_args})
# An installed package cannot carry a FetchContent asio, so such a host has nothing to consume.
if(_step_output MATCHES "asio fetched at")
    message(STATUS "PLEXUS_SKIP: installed asio is not resolvable on this host")
    return()
endif()

run_step("plexus build" ${CMAKE_COMMAND} --build "${WORK_DIR}/build" --parallel 2)
run_step("plexus install" ${CMAKE_COMMAND} --install "${WORK_DIR}/build")

run_step("consumer configure" ${CMAKE_COMMAND}
    -S "${PROBE_DIR}" -B "${WORK_DIR}/consumer" ${_toolchain_args}
    "-DCMAKE_PREFIX_PATH=${WORK_DIR}/prefix")
run_step("consumer build" ${CMAKE_COMMAND} --build "${WORK_DIR}/consumer" --target install_consumer)
