# The nodes x topics x payload scalability probe. Single process, public API only,
# linking the core and the header-only in-process backend.

add_executable(plexus_scale plexus_scale.cpp)
target_link_libraries(plexus_scale PRIVATE plexus::plexus plexus::inproc)
