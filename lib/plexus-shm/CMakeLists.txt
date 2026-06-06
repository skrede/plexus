# Gated, compiled POSIX shared-memory backend (linux-first): the region broker,
# the verbatim-ported futex notifier primitive, and the same-host fingerprint.
# The header-only core (plexus, plexus-wire) never pulls <sys/mman.h> or the
# futex syscall — this target, and only this target, carries the POSIX bodies,
# and only when the backend is ON. Auto-discovered by the root lib glob;
# self-gated so the core stays shm-free when PLEXUS_ENABLE_SHM_BACKEND is OFF.
#
# The option is declared HERE (not in the root CMakeLists.txt) so the target
# registers by adding this fragment alone, matching the discovery-adapter
# precedent.
option(PLEXUS_ENABLE_SHM_BACKEND "Build the gated POSIX shared-memory backend (linux-first)" OFF)

if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# STATIC archive — deliberately NOT OBJECT. Unlike plexus-crypto (whose single
# TU transitively instantiates asio's io_context, forcing the OBJECT/COMDAT-fold
# wiring), this backend never touches asio: the reactor bridge is the
# plexus-asio adapter's job, not this target's. With no asio-bearing symbols a
# second STATIC archive cannot fold against the asio backend's copy, so a plain
# archive is the honest shape. Link deps are PUBLIC plexus::plexus + plexus::wire
# — explicitly NOT asio.
add_library(plexus-shm STATIC
    src/shm_backend_version.cpp
    src/region_handle.cpp
    src/posix_shm_region_broker.cpp)
add_library(plexus::shm ALIAS plexus-shm)

target_include_directories(plexus-shm PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-shm PUBLIC
    plexus::plexus
    plexus::wire)

target_compile_features(plexus-shm PUBLIC cxx_std_20)
