add_executable(plexus_scale plexus_scale.cpp)
target_link_libraries(plexus_scale PRIVATE plexus::plexus plexus::inproc plexus::discovery)
