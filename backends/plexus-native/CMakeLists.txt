# The only target carrying the OS shared-memory bodies, so the core never pulls <sys/mman.h> or the futex syscall.
# Its option is declared at root because the asio backend, globbed before this fragment, reads it too.
if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# STATIC, not OBJECT like plexus-crypto: this backend never touches asio, so its archive has no asio symbols to fold
# against the asio backend's copy. The futex notifier primitive is Linux-only because Windows and macOS carry their
# wake in the asio reactor bridge.
add_library(plexus-native STATIC
    src/shm_backend_version.cpp
    src/machine_fingerprint.cpp)
add_library(plexus::native ALIAS plexus-native)

if(WIN32)
    target_sources(plexus-native PRIVATE
        src/win_slot_mutex.cpp
        src/win_region_handle.cpp
        src/win_shm_region_broker.cpp)
else()
    target_sources(plexus-native PRIVATE
        src/region_handle.cpp
        src/posix_slot_lock.cpp
        src/posix_sidecar_lock.cpp
        src/posix_shm_status.cpp
        src/posix_shm_region_broker.cpp)
    if(LINUX)
        target_sources(plexus-native PRIVATE
            src/futex_notifier_primitive.cpp)
    endif()
endif()

target_include_directories(plexus-native PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-native PUBLIC
    plexus::core
    plexus::shm
    plexus::wire
    plexus::stream)

target_compile_definitions(plexus-native PRIVATE PLEXUS_PROJECT_VERSION="${PROJECT_VERSION}")

target_compile_features(plexus-native PUBLIC cxx_std_20)

set_target_properties(plexus-native PROPERTIES EXPORT_NAME native)

include(GNUInstallDirs)

# Every interface dependency already ships in plexusTargets, so the native config block needs no find_dependency.
install(TARGETS plexus-native EXPORT plexusNativeTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusNativeTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-native-targets.cmake)
