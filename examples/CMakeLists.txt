# Runnable examples — the public face of the API. Each example is one terminal binary,
# grouped by the pattern it demonstrates. The helper below is defined here and is visible
# to every subdirectory; each subdirectory guards itself on the backends it needs, so the
# tree builds with whatever backends are enabled.

function(plexus_example name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE ${ARGN})
endfunction()

add_subdirectory(pub_sub)
add_subdirectory(req_res)
add_subdirectory(inproc)
add_subdirectory(logging)
add_subdirectory(recording)
add_subdirectory(same_host)
add_subdirectory(qos)
add_subdirectory(mcu)
