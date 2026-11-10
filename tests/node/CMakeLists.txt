set(PLEXUS_TEST_PREFIX "node.")
set(PLEXUS_TEST_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/common)

add_subdirectory(facade)
add_subdirectory(self_route)
add_subdirectory(lifecycle)
add_subdirectory(discovery)
# The shm self-carrier's self-ring wake rides the Linux io_uring/futex reactor bridge, so it is excluded from Windows.
if(PLEXUS_ENABLE_SHM_BACKEND AND NOT WIN32)
    add_subdirectory(same_host)
endif()
add_subdirectory(link_checks)
