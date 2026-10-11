foreach(_input IN ITEMS PLEXUS_SOURCE_DIR PROBE_DIR WORK_DIR PROBE_GENERATOR PROBE_CXX_COMPILER)
    if(NOT DEFINED ${_input})
        message(FATAL_ERROR "run_shm_off.cmake needs -D${_input}")
    endif()
endforeach()

file(REMOVE_RECURSE "${WORK_DIR}")

set(_configure_args
    -S "${PROBE_DIR}" -B "${WORK_DIR}/build" -G "${PROBE_GENERATOR}"
    "-DCMAKE_CXX_COMPILER=${PROBE_CXX_COMPILER}"
    "-DPLEXUS_SOURCE_DIR=${PLEXUS_SOURCE_DIR}"
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=OFF
    -DPLEXUS_BUILD_TESTS=OFF -DPLEXUS_BUILD_EXAMPLES=OFF)
if(PROBE_CXX_COMPILER_LAUNCHER)
    list(APPEND _configure_args "-DCMAKE_CXX_COMPILER_LAUNCHER=${PROBE_CXX_COMPILER_LAUNCHER}")
endif()
if(ASIO_SOURCE_DIR)
    list(APPEND _configure_args "-DFETCHCONTENT_SOURCE_DIR_ASIO=${ASIO_SOURCE_DIR}")
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
endfunction()

run_step("nested configure" ${CMAKE_COMMAND} ${_configure_args})
run_step("shm_off_consumer build" ${CMAKE_COMMAND} --build "${WORK_DIR}/build" --target shm_off_consumer)
