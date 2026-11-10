if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "shm.")
set(PLEXUS_TEST_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/common)

find_package(Threads REQUIRED)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBURING QUIET IMPORTED_TARGET liburing)
endif()

# io_uring_prep_futex_wait needs liburing 2.6 and the spike calls it directly, so it checks the capability, not the
# presence. The bridge tests stay under LIBURING_FOUND: ring_notifier falls back to its threaded floor without it.
if(LIBURING_FOUND)
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES ${LIBURING_INCLUDE_DIRS})
    check_cxx_source_compiles([[
        #include <liburing.h>
        int main() { io_uring_prep_futex_wait(static_cast<io_uring_sqe *>(nullptr), static_cast<unsigned *>(nullptr), 0, 0, 0, 0); }
    ]] PLEXUS_SHM_TESTS_HAVE_FUTEX)
    unset(CMAKE_REQUIRED_INCLUDES)
endif()

if(LIBURING_FOUND)
    if(PLEXUS_SHM_TESTS_HAVE_FUTEX)
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found with futex-wait — io_uring spike + reactor bridge enabled")
    else()
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found without futex-wait (< 2.6) — reactor bridge on the threaded floor, io_uring spike skipped")
    endif()
else()
    message(STATUS "plexus shm tests: liburing absent — io_uring futex spike + reactor bridge skipped (non-gating)")
endif()

add_subdirectory(ring)
add_subdirectory(registry)
add_subdirectory(broker)
add_subdirectory(notifier)
add_subdirectory(coordinator)
add_subdirectory(same_host)
