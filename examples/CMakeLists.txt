# Runnable examples — the public face of the API. Each example is one terminal binary,
# grouped by the pattern it demonstrates. The helper below is defined here and is visible
# to every subdirectory; each subdirectory guards itself on the backends it needs, so the
# tree builds with whatever backends are enabled.

function(plexus_example name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE ${ARGN})
endfunction()

add_subdirectory(discovery)
add_subdirectory(pub_sub)
add_subdirectory(req_res)
add_subdirectory(inproc)
add_subdirectory(intra_node)
add_subdirectory(logging)
add_subdirectory(recording)
add_subdirectory(same_host)
add_subdirectory(qos)
add_subdirectory(mcu)

# The mDNS interop example is opt-in: it FetchContents mdnspp and bumps to C++23, so
# it stays off the default examples build (which is mdnspp-free and C++20).
option(PLEXUS_BUILD_MDNS_EXAMPLE "Build the opt-in real-mDNS interop example" OFF)
if(PLEXUS_BUILD_MDNS_EXAMPLE)
    add_subdirectory(mdns_interop)
endif()
