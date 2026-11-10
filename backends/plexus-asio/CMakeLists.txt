# The only target that pulls standalone asio in, so the header-only core stays asio-free.
if(NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

add_library(plexus-asio INTERFACE)
add_library(plexus::asio ALIAS plexus-asio)

target_include_directories(plexus-asio INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-asio INTERFACE
    plexus::plexus
    plexus::core
    plexus::wire
    plexus::stream
    plexus::datagram
    plexus::discovery
    plexus_asio_dep)

# The multicast egress resolver (detail/interface_resolve.h) calls GetAdaptersAddresses on Windows,
# which lives in iphlpapi; getifaddrs is in libc on Linux/macOS and needs no extra link.
target_link_libraries(plexus-asio INTERFACE $<$<PLATFORM_ID:Windows>:iphlpapi>)

target_compile_features(plexus-asio INTERFACE cxx_std_20)

set_target_properties(plexus-asio PROPERTIES EXPORT_NAME asio)

include(GNUInstallDirs)

# install(EXPORT) hard-errors unless every interface target lands in some export set, so plexus_asio_dep joins this
# one. Its installed flavor carries no asio include path; an installed consumer resolves asio through the config
# package's find_dependency.
install(TARGETS plexus-asio plexus_asio_dep EXPORT plexusAsioTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusAsioTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-asio-targets.cmake)

# shm-include/ is a separate root so a bare plexus::asio consumer has no path to plexus/asio/shm/*, a real compile
# barrier; PLEXUS_SAME_HOST_SHM_BACKEND travels with that path so the same-host composition selects shm exactly when
# it is reachable. Linking plexus::asio gives the composition's cross-root includes their include/ path back.
if(PLEXUS_ENABLE_SHM_BACKEND)
    add_library(plexus-asio-shm INTERFACE)
    add_library(plexus::asio::shm ALIAS plexus-asio-shm)

    target_include_directories(plexus-asio-shm INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/shm-include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    target_compile_definitions(plexus-asio-shm INTERFACE PLEXUS_SAME_HOST_SHM_BACKEND)

    target_link_libraries(plexus-asio-shm INTERFACE
        plexus::asio
        plexus::shm
        plexus::native)

    # linux/uring_notifier.h calls io_uring_* directly, so liburing is a usage requirement and a consumer never
    # rediscovers it.
    if(LINUX)
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)
        target_link_libraries(plexus-asio-shm INTERFACE PkgConfig::LIBURING)

        # io_uring_prep_futex_wait landed in liburing 2.6 and Ubuntu 24.04 ships 2.5. A probe rather than a version
        # gate picks up a distro backport; ring_notifier.h selects the io_uring bridge or its portable threaded floor.
        include(CheckCXXSourceCompiles)
        set(CMAKE_REQUIRED_INCLUDES ${LIBURING_INCLUDE_DIRS})
        check_cxx_source_compiles([[
            #include <liburing.h>
            int main() { io_uring_prep_futex_wait(static_cast<io_uring_sqe *>(nullptr), static_cast<unsigned *>(nullptr), 0, 0, 0, 0); }
        ]] PLEXUS_LIBURING_HAS_FUTEX)
        unset(CMAKE_REQUIRED_INCLUDES)
        target_compile_definitions(plexus-asio-shm INTERFACE
            PLEXUS_HAS_IO_URING_FUTEX=$<BOOL:${PLEXUS_LIBURING_HAS_FUTEX}>)
    endif()

    # windows/event_doorbell.h builds its owner-only event security descriptor through advapi32.
    if(WIN32)
        target_link_libraries(plexus-asio-shm INTERFACE advapi32)
    endif()

    target_compile_features(plexus-asio-shm INTERFACE cxx_std_20)

    set_target_properties(plexus-asio-shm PROPERTIES EXPORT_NAME asio-shm)

    install(TARGETS plexus-asio-shm EXPORT plexusAsioShmTargets)
    install(DIRECTORY shm-include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    install(EXPORT plexusAsioShmTargets
        NAMESPACE plexus::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
        FILE plexus-asio-shm-targets.cmake)
endif()
