# Gated, compiled asio TCP backend: the production transport half of the slice.
# The header-only core (plexus-core, plexus-wire) never links asio — this target, and
# only this target, pulls standalone asio in, and only when the backend is ON.
# Auto-discovered by the root lib glob; self-gated so the core stays asio-free
# when PLEXUS_ENABLE_ASIO_BACKEND is OFF.
if(NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

# Shared, pinned standalone-asio dependency (one copy across plexus + mdnspp).
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

add_library(plexus-asio INTERFACE)
add_library(plexus::asio ALIAS plexus-asio)

target_include_directories(plexus-asio INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-asio INTERFACE
    plexus::core
    plexus::wire
    plexus_asio_dep)

target_compile_features(plexus-asio INTERFACE cxx_std_20)

set_target_properties(plexus-asio PROPERTIES EXPORT_NAME asio)

include(GNUInstallDirs)

# plexus_asio_dep is in plexus-asio's link interface but lives outside plexusTargets;
# install(EXPORT) hard-errors unless every interface target lands in some export set, so
# it joins this set. The installed flavor carries no asio include path of its own (the
# FetchContent branch's path is $<BUILD_INTERFACE>-guarded); an installed consumer resolves
# asio via the config package's find_dependency on the find_package/pkg-config branch.
install(TARGETS plexus-asio plexus_asio_dep EXPORT plexusAsioTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusAsioTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-asio-targets.cmake)
