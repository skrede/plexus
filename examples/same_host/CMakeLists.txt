# Both examples need a POSIX host (getpid, /tmp). same_host_node also builds without shm, and puts shm in front of its
# other transports whenever the shm target is linked.
if(TARGET plexus::asio::shm AND NOT WIN32)
    plexus_example(shm_local_node plexus::plexus plexus::asio plexus::asio::shm plexus::native)
endif()

if(TARGET plexus::asio AND NOT WIN32)
    plexus_example(same_host_node plexus::plexus plexus::asio)
    if(TARGET plexus::asio::shm AND TARGET plexus::native)
        target_link_libraries(same_host_node PRIVATE plexus::asio::shm plexus::native)
    endif()
endif()
