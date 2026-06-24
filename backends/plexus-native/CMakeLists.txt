# The native (direct-OS) backend: the gated, compiled POSIX bodies plexus reaches
# for without an intermediary library (the counterpart to plexus-asio). Today it
# carries the POSIX shared-memory mechanism — the region broker, the futex notifier
# primitive, and the same-host fingerprint — and only those bodies are compiled,
# behind the gate. The header-only core (plexus, plexus-wire) never pulls
# <sys/mman.h> or the futex syscall — this target, and only this target, carries the
# POSIX bodies, and only when the backend is ON. Auto-discovered by the root
# backends glob; self-gated so the core stays shm-free when PLEXUS_ENABLE_SHM_BACKEND
# is OFF.
#
# The option is declared at root (ahead of the lib glob) so every fragment —
# including the asio backend, globbed before this one — reads it; here it is only
# read, via the early return.
if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# STATIC archive — deliberately NOT OBJECT. Unlike plexus-crypto (whose single
# TU transitively instantiates asio's io_context, forcing the OBJECT/COMDAT-fold
# wiring), this backend never touches asio: the reactor bridge is the
# plexus-asio adapter's job, not this target's. With no asio-bearing symbols a
# second STATIC archive cannot fold against the asio backend's copy, so a plain
# archive is the honest shape. Link deps are PUBLIC plexus::core + plexus::wire
# + plexus::shm (the family concept headers it satisfies) — explicitly NOT asio.
add_library(plexus-native STATIC
    src/shm_backend_version.cpp
    src/region_handle.cpp
    src/posix_shm_region_broker.cpp
    src/futex_notifier_primitive.cpp
    src/machine_fingerprint.cpp)
add_library(plexus::native ALIAS plexus-native)

target_include_directories(plexus-native PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-native PUBLIC
    plexus::core
    plexus::shm
    plexus::wire)

target_compile_features(plexus-native PUBLIC cxx_std_20)

set_target_properties(plexus-native PROPERTIES EXPORT_NAME native)

include(GNUInstallDirs)

# A plain STATIC archive whose only interface deps (plexus::core, plexus::wire) already
# ship in plexusTargets — no find_dependency is needed in the native config block.
install(TARGETS plexus-native EXPORT plexusNativeTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusNativeTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-native-targets.cmake)
