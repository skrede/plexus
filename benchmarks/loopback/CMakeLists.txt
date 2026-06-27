# The cross-transport same-node self-delivery probe. Single process, public API only. The
# intra-node lane links the header-only in-process backend; the shm-self lane links the shm node
# surface and is compiled only when the shm backend gate is on (else it skips at runtime).

add_executable(plexus_self_loopback plexus_self_loopback.cpp)
target_include_directories(plexus_self_loopback PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(plexus_self_loopback PRIVATE plexus::plexus plexus::inproc)

if(PLEXUS_ENABLE_SHM_BACKEND)
    target_compile_definitions(plexus_self_loopback PRIVATE PLEXUS_ENABLE_SHM_BACKEND)
    target_link_libraries(plexus_self_loopback PRIVATE
        plexus::shm
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native)
endif()
