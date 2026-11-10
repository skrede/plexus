# Without the shm backend the shm-self lane is compiled out and skips at runtime.

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
