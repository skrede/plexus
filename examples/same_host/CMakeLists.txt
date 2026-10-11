# Same-host nodes over AF_UNIX + plain TCP, crypto-free. shm_local_node composes the Linux shm member
# directly; same_host_node is the portable composition, which puts shm in front whenever the shm
# target is linked and needs only a POSIX host (getpid, /tmp).
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND TARGET plexus::asio::shm)
    plexus_example(shm_local_node plexus::plexus plexus::asio plexus::asio::shm plexus::native)
endif()

if(TARGET plexus::asio AND NOT WIN32)
    plexus_example(same_host_node plexus::plexus plexus::asio)
    if(TARGET plexus::asio::shm AND TARGET plexus::native)
        target_link_libraries(same_host_node PRIVATE plexus::asio::shm plexus::native)
    endif()
endif()
